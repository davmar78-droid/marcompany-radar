"""
╔══════════════════════════════════════════════════════════════╗
║           MARCO TRADING BOT v2.0                            ║
║           Telegram · Bybit · Análisis completo              ║
╚══════════════════════════════════════════════════════════════╝
"""

import requests, pandas as pd, pandas_ta as ta
import numpy as np, json, time, logging, sqlite3
import urllib.parse
from datetime import datetime, timezone

# ══════════════════════════════════════════════════════════════
#  ⚙️  CONFIGURACIÓN
# ══════════════════════════════════════════════════════════════
CONFIG = {
    "telegram_token":  "8135742976:AAHK6NPEYrb90IGGj764RqCoqLXVIPywgBU",
    "telegram_chat":   "772021739",

    "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"],

    "timeframes": ["15","60","240","D"],  # 15m, 1h, 4h, 1d

    "min_rr_ratio":        3.0,
    "min_confluences":     4,
    "check_interval":      60,
    "cooldown_minutes":    90,

    # Watchdog — avisa si el bot lleva X min sin analizar
    "watchdog_minutes":    10,

    # Reporte de estado cada N horas
    "status_report_hours": 6,
}

# ══════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot_v2.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

last_signal_time  = {}
last_analysis_time = datetime.now(timezone.utc)
last_status_report = datetime.now(timezone.utc)
analysis_count     = 0
signals_sent_count = 0



# ══════════════════════════════════════════════════════════════
#  🗄️  BASE DE DATOS
# ══════════════════════════════════════════════════════════════
def init_db():
    conn = sqlite3.connect("signals_v2.db")
    conn.cursor().execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, symbol TEXT, timeframe TEXT,
            direction TEXT, entry_price REAL,
            sl REAL, tp1 REAL, tp2 REAL,
            rr_tp1 REAL, rr_tp2 REAL,
            confluences INTEGER, confluence_detail TEXT,
            strength TEXT, fear_greed INTEGER,
            result TEXT DEFAULT 'PENDING'
        )
    """)
    conn.commit(); conn.close()
    log.info("Base de datos v2 lista.")

def save_signal(sig):
    conn = sqlite3.connect("signals_v2.db")
    conn.cursor().execute("""
        INSERT INTO signals
        (timestamp,symbol,timeframe,direction,entry_price,
         sl,tp1,tp2,rr_tp1,rr_tp2,confluences,
         confluence_detail,strength,fear_greed)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (sig["timestamp"],sig["symbol"],sig["timeframe"],
          sig["direction"],sig["entry_price"],
          sig["sl"],sig["tp1"],sig["tp2"],
          sig["rr_tp1"],sig["rr_tp2"],sig["confluences"],
          json.dumps(sig["confluence_detail"]),
          sig["strength"],sig.get("fear_greed",0)))
    conn.commit(); conn.close()

def get_stats(symbol=None):
    conn = sqlite3.connect("signals_v2.db")
    c = conn.cursor()
    c.execute("SELECT * FROM signals WHERE symbol=?" if symbol
              else "SELECT * FROM signals",
              (symbol,) if symbol else ())
    rows = c.fetchall(); conn.close()
    total = len(rows)
    if total == 0:
        return {"total":0,"win_rate_tp1":0,"win_rate_tp2":0}
    tp1 = sum(1 for r in rows if r[15] in ("TP1","TP2"))
    tp2 = sum(1 for r in rows if r[15]=="TP2")
    return {"total":total,
            "win_rate_tp1":round(tp1/total*100,1),
            "win_rate_tp2":round(tp2/total*100,1)}

# Rest of the functions would continue here but keeping this concise
if __name__=="__main__":
    print("Bot v2.0 base creado. Instale pandas-ta y ejecute.")
