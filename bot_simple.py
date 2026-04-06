import requests, pandas as pd, pandas_ta as ta, numpy as np, time, logging
from datetime import datetime, timezone

CONFIG = {
    "telegram_token": "8135742976:AAHK6NPEYrb90IGGj764RqCoqLXVIPywgBU",
    "telegram_chats": ["-1003893933581", "772021739"],
    "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"],
    "timeframes": ["15", "60"],
    "min_rr_ratio": 3.0,
    "min_confluences": 3,
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def send_telegram(message):
    token = CONFIG["telegram_token"]
    chats = CONFIG["telegram_chats"]
    for chat in chats:
        try:
            r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat, "text": message}, timeout=15)
            if r.status_code == 200:
                log.info(f"✅ Telegram {chat} OK")
            else:
                log.warning(f"❌ Error {chat}: {r.status_code}")
        except Exception as e:
            log.error(f"Error {chat}: {e}")

def run():
    log.info("🚀 Bot simple iniciado")
    send_telegram("Bot iniciado correctamente")
    
    while True:
        log.info("Corriendo...")
        time.sleep(60)

if __name__ == "__main__":
    run()
