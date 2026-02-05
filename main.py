import asyncio
import logging
import os
import sys
from typing import Dict, Any, Optional, Tuple

import httpx

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)
logging.getLogger('backoff').addHandler(logging.StreamHandler())


API_SERVER = os.getenv("API_SERVER")
BOT_ID = os.getenv("BOT_ID")  # Telegram bot token
CHAT_ID = os.getenv("CHAT_ID")
CHAIN_API = os.getenv("CHAIN_API")  # Chain REST API, e.g. http://chain-node:1317
PARTICIPANT_ADDRESS = os.getenv("PARTICIPANT_ADDRESS")  # Адрес участника для мониторинга confirmation weight

if not API_SERVER or not BOT_ID or not CHAT_ID:
    logging.error("Environment variables API_SERVER, BOT_ID and CHAT_ID must be set")
    sys.exit(1)

if PARTICIPANT_ADDRESS:
    if not CHAIN_API:
        CHAIN_API = f"http://{API_SERVER}:1317"
        logging.info(f"CHAIN_API not set, defaulting to {CHAIN_API}")
    logging.info(f"Confirmation weight monitoring enabled for {PARTICIPANT_ADDRESS}")


async def send_telegram(text: str) -> None:
    url = f"https://api.telegram.org/bot{BOT_ID}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
    except Exception as e:
        # Если даже уведомление не получилось отправить – хотя бы лог в stderr
        logging.error(f"Failed to send telegram message: {e}")


async def fetch_report(client: httpx.AsyncClient) -> Dict[str, Any]:
    url = f"http://{API_SERVER}:9200/admin/v1/setup/report"
    resp = await client.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _get(d: dict, *keys, default=None):
    """Получить значение по первому найденному ключу (для совместимости camelCase/snake_case)."""
    for key in keys:
        if key in d:
            return d[key]
    return default


async def fetch_confirmation_weight(client: httpx.AsyncClient) -> Optional[Tuple[int, int, int, int]]:
    """Получить confirmation weight участника из chain REST API.

    Returns (confirmation_weight, weight, total_weight, epoch_index) или None если участник не найден.
    """
    url = f"{CHAIN_API}/productscience/inference/inference/current_epoch_group_data"
    resp = await client.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    epoch_data = _get(data, "epoch_group_data", "epochGroupData", default=data)
    weights = _get(epoch_data, "validation_weights", "validationWeights", default=[])
    total_weight = int(_get(epoch_data, "total_weight", "totalWeight", default=0))
    epoch_index = int(_get(epoch_data, "epoch_index", "epochIndex", default=0))

    for vw in weights:
        addr = _get(vw, "member_address", "memberAddress", default="")
        if addr == PARTICIPANT_ADDRESS:
            cw = int(_get(vw, "confirmation_weight", "confirmationWeight", default=0))
            w = int(_get(vw, "weight", default=0))
            return (cw, w, total_weight, epoch_index)

    return None


async def monitor() -> None:
    prev_statuses: Dict[str, str] = {}
    prev_missed_requests: Optional[int] = None
    prev_confirmation_weight: Optional[int] = None
    prev_cw_epoch: Optional[int] = None

    async with httpx.AsyncClient(timeout=10) as client:
        while True:
            logging.info("Checking status...")
            try:
                report = await fetch_report(client)
                checks = report.get("checks", [])

                for check in checks:
                    cid = check.get("id")
                    status = check.get("status")
                    message = check.get("message", "")
                    details = check.get("details", {}) or {}

                    # 1. Пропускаем глючный consensus_key_match
                    if cid == "consensus_key_match":
                        continue

                    # 2. Специальная логика для missed_requests_threshold
                    if cid == "missed_requests_threshold":
                        missed = details.get("missed_requests")
                        total = details.get("total_requests")
                        missed_pct = details.get("missed_percentage")

                        if isinstance(missed, int) and prev_missed_requests is not None:
                            if missed > prev_missed_requests:
                                await send_telegram(
                                    f"[missed_requests_threshold] Missed requests increased: "
                                    f"{prev_missed_requests} -> {missed} "
                                    f"(total={total}, missed%={missed_pct})"
                                )
                        if isinstance(missed, int):
                            prev_missed_requests = missed
                        continue

                    # 3. Все остальные проверки:
                    #    уведомляем, если статус стал отличным от PASS
                    if status != "PASS":
                        prev_status = prev_statuses.get(cid)
                        # Чтобы не спамить, шлем только при переходе из PASS/unknown в non-PASS
                        if prev_status is None or prev_status == "PASS":
                            # Отдельный текст для mlnode_*
                            if cid and cid.startswith("mlnode_"):
                                node_id = details.get("id") or cid.removeprefix("mlnode_")
                                host = details.get("host", "unknown-host")
                                await send_telegram(
                                    f"[{cid}] ML node problem on {host}/{node_id}: {message}"
                                )
                            else:
                                # В случае ошибки отправляем ее id и message
                                await send_telegram(f"[{cid}] {status}: {message}")

                    # Обновляем состояние по статусу
                    if cid is not None and status is not None:
                        prev_statuses[cid] = status

            except Exception as e:
                # Ошибка самого скрипта / HTTP / JSON
                await send_telegram(f"[script_error] {type(e).__name__}: {e}")

            # Проверка confirmation weight
            if PARTICIPANT_ADDRESS:
                try:
                    result = await fetch_confirmation_weight(client)
                    if result is not None:
                        cw, w, total_w, epoch_idx = result

                        # Сброс при смене эпохи
                        if prev_cw_epoch is not None and epoch_idx != prev_cw_epoch:
                            prev_confirmation_weight = None
                            logging.info(f"Epoch changed: {prev_cw_epoch} -> {epoch_idx}, resetting CW tracking")

                        if prev_confirmation_weight is not None and cw < prev_confirmation_weight:
                            if prev_confirmation_weight > 0:
                                pct_change = (cw - prev_confirmation_weight) / prev_confirmation_weight * 100
                            else:
                                pct_change = 0.0
                            share = (cw / total_w * 100) if total_w > 0 else 0.0
                            await send_telegram(
                                f"[confirmation_weight] Decreased: "
                                f"{prev_confirmation_weight} -> {cw} ({pct_change:+.1f}%) "
                                f"(weight={w}, total={total_w}, share={share:.1f}%)"
                            )

                        prev_confirmation_weight = cw
                        prev_cw_epoch = epoch_idx
                except Exception as e:
                    logging.warning(f"Failed to check confirmation weight: {e}")

            # Ждем минуту до следующей проверки
            await asyncio.sleep(60)


def main() -> None:
    try:
        asyncio.run(monitor())
    except KeyboardInterrupt:
        logging.error("Stopped by user")


if __name__ == "__main__":
    main()