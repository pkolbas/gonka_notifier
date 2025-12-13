import asyncio
import logging
import os
import sys
from typing import Dict, Any, Optional

import httpx

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)


API_SERVER = os.getenv("API_SERVER")
BOT_ID = os.getenv("BOT_ID")  # Telegram bot token
CHAT_ID = os.getenv("CHAT_ID")
MISSED_PCT_THRESHOLD = float(os.getenv("MISSED_PCT_THRESHOLD", "3.0"))
PCT_DECIMALS = int(os.getenv("MISSED_PCT_DECIMALS", "2"))  # антидребезг: сколько знаков учитывать

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


def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def round_pct(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return round(x, PCT_DECIMALS)


def fmt_pct(x: Optional[float]) -> str:
    if x is None:
        return "n/a"
    return f"{x:.{PCT_DECIMALS}f}%"


async def monitor() -> None:
    prev_statuses: Dict[str, str] = {}

    # Храним последнее ОТПРАВЛЕННОЕ значение missed% (уже округлённое)
    last_sent_missed_pct: Optional[float] = None

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

                    # Игнорируем глючные проверки
                    if cid in {"consensus_key_match", "validator_in_set"}:
                        continue

                    # missed_requests_threshold: уведомлять по росту процента (с антидребезгом)
                    if cid == "missed_requests_threshold":
                        missed = details.get("missed_requests")
                        total = details.get("total_requests")

                        missed_pct_raw = safe_float(details.get("missed_percentage"))
                        if missed_pct_raw is None:
                            try:
                                if isinstance(missed, (int, float)) and isinstance(total, (int, float)) and total:
                                    missed_pct_raw = (float(missed) / float(total)) * 100.0
                            except Exception:
                                missed_pct_raw = None

                        missed_pct = round_pct(missed_pct_raw)
                        threshold = round_pct(MISSED_PCT_THRESHOLD)

                        # ниже порога — игнорируем полностью и last_sent не трогаем
                        if missed_pct is None or threshold is None or missed_pct < threshold:
                            continue

                        # сравниваем только округлённые значения
                        if last_sent_missed_pct is None or missed_pct > last_sent_missed_pct:
                            await send_telegram(
                                f"[missed_requests_threshold] Missed% increased: "
                                f"{fmt_pct(last_sent_missed_pct)} -> {fmt_pct(missed_pct)} "
                                f"(missed={missed}, total={total}, threshold={fmt_pct(threshold)})"
                            )
                            last_sent_missed_pct = missed_pct

                        continue

                    # Остальные проверки: уведомление при переходе в non-PASS
                    if status != "PASS":
                        prev_status = prev_statuses.get(cid)
                        if prev_status is None or prev_status == "PASS":
                            if cid and cid.startswith("mlnode_"):
                                node_id = details.get("id") or cid.removeprefix("mlnode_")
                                host = details.get("host", "unknown-host")
                                await send_telegram(f"[{cid}] ML node problem on {host}/{node_id}: {message}")
                            else:
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