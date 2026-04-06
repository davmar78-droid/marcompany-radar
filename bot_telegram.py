import requests, pandas as pd, pandas_ta as ta, numpy as np, json, time, logging, sqlite3, urllib.parse
from datetime import datetime, timezone

CONFIG = {
    "telegram_token": "8135742976:AAHK6NPEYrb90IGGj764RqCoqLXVIPywgBU",
    "telegram_chats": ["-1003893933581", "772021739"],
    "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"],
    "timeframes": ["15", "60", "240", "D"],
    "min_rr_ratio": 3.0, "min_confluences": 4, "check_interval": 60, "cooldown_minutes": 30,
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
last_signal_time = {}

def send_telegram(message):
    token = CONFIG["telegram_token"]
    chats = CONFIG["telegram_chats"]
    for chat in chats:
        try:
            r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat, "text": message}, timeout=15)
            if r.status_code == 200:
                log.info(f"✅ Telegram enviado a {chat}")
            else:
                log.warning(f"Error Telegram {chat}: {r.status_code}")
        except Exception as e:
            log.error(f"Error Telegram {chat}: {e}")

def should_send(symbol, tf):
    key = f"{symbol}_{tf}"
    last = last_signal_time.get(key)
    if last is None: return True
    return (datetime.now(timezone.utc) - last).total_seconds() / 60 >= CONFIG["cooldown_minutes"]

def run():
    log.info("🚀 MARCO BOT TELEGRAM INICIADO")
    send_telegram("🤖 Marco Bot Telegram iniciado\n✅ Funcionando correctamente")
    
    while True:
        log.info("Bot corriendo - enviando test...")
        send_telegram("🔥 TEST - Bot funcionando correctamente")
        time.sleep(60)

if __name__ == "__main__":
    run()
