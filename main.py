import asyncio
import logging
import os
import sys
from typing import Dict, Any, Optional

import httpx

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)
logging.getLogger('backoff').addHandler(logging.StreamHandler())


API_SERVER = os.getenv("API_SERVER")
BOT_ID = os.getenv("BOT_ID")  # Telegram bot token
CHAT_ID = os.getenv("CHAT_ID")

if not API_SERVER or not BOT_ID or not CHAT_ID:
    logging.error("Environment variables API_SERVER, BOT_ID and CHAT_ID must be set")
    sys.exit(1)


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


async def monitor() -> None:
    prev_statuses: Dict[str, str] = {}
    prev_missed_requests: Optional[int] = None

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

            # Ждем минуту до следующей проверки
            await asyncio.sleep(60)


def main() -> None:
    try:
        asyncio.run(monitor())
    except KeyboardInterrupt:
        logging.error("Stopped by user")


if __name__ == "__main__":
    main()