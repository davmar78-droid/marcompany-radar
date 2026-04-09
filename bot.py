import requests
import pandas as pd
import pandas_ta as ta
import numpy as np
import json, time, logging, sqlite3, subprocess, threading
import concurrent.futures
from datetime import datetime, timezone

# ══════════════════════════════════════════════════════
#  ⚙️  CONFIGURACIÓN
# ══════════════════════════════════════════════════════
CONFIG = {
    "telegram_token":   "8135742976:AAHK6NPEYrb90IGGj764RqCoqLXVIPywgBU",
    "telegram_chats":   ["-1003893933581", "772021739"],
    "symbols":          ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"],
    "timeframes":       ["15", "60", "240", "D"],
    "min_rr_tp1":       2.0,
    "min_rr_tp2":       4.0,
    "min_confluences":  5,
    "check_interval":   120,
    "cooldown_minutes": 45,
    # ══ ZONA HORARIA ═══════════════════════════════════
    "utc_offset":        2,      # España verano: 2 | España invierno: 1
    "capital_total":     1000,   # ← Pon aquí tu capital en USDT
    "riesgo_pct":        1.0,    # ← % del capital a arriesgar por operación
    "leverage_muy_alta": 10,     # Apalancamiento para señales MUY ALTA
    "leverage_alta":     5,      # Apalancamiento para señales ALTA
    "leverage_media":    3,      # Apalancamiento para señales MEDIA
    "leverage_baja":     1,      # Apalancamiento para señales BAJA
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)
last_signal_time = {}
recent_signals_cache = []
db_lock = threading.Lock()  # Protege SQLite en análisis paralelo
bot_running = True          # Flag para pausar/reanudar el bot
recent_directions = {}      # {symbol: {"direction": "LONG"/"SHORT", "time": datetime}}
recent_directions_lock = threading.Lock()

# ══════════════════════════════════════════════════════
#  🎭  DETECCIÓN DE MANIPULACIÓN
# ══════════════════════════════════════════════════════
def detect_manipulation(df):
    """
    Detecta señales de manipulación de precio típicas en futuros:
    - Wick extremo: mecha > 3x el cuerpo (caza de liquidez)
    - Stop hunt: wick que rompe soporte/resistencia y vuelve rápido
    - Spike de volumen sin continuación
    Devuelve (bool, descripcion)
    """
    if len(df) < 5:
        return False, ""

    c    = df.iloc[-1]
    prev = df.iloc[-2]
    body = abs(c["close"] - c["open"])
    rng  = c["high"] - c["low"]
    upper = c["high"] - max(c["close"], c["open"])
    lower = min(c["close"], c["open"]) - c["low"]

    warnings = []

    # Wick extremo — mecha > 3x el cuerpo
    if body > 0 and rng > 0:
        if upper > body * 3 and upper > rng * 0.6:
            warnings.append("mecha superior extrema (posible trampa alcista)")
        if lower > body * 3 and lower > rng * 0.6:
            warnings.append("mecha inferior extrema (posible trampa bajista)")

    # Vela tipo doji con volumen muy alto — indecision manipulada
    vol    = c.get("volume", 0)
    vol_ma = df["volume"].tail(20).mean()
    if body < rng * 0.1 and vol > vol_ma * 2.5:
        warnings.append("doji con volumen extremo (posible manipulacion)")

    # Spike que rompe y regresa — comparar con velas previas
    prev_high = df["high"].tail(10).iloc[:-1].max()
    prev_low  = df["low"].tail(10).iloc[:-1].min()
    if c["high"] > prev_high * 1.003 and c["close"] < prev_high:
        warnings.append("spike sobre resistencia sin cierre (stop hunt)")
    if c["low"] < prev_low * 0.997 and c["close"] > prev_low:
        warnings.append("spike bajo soporte sin cierre (stop hunt)")

    if warnings:
        return True, " / ".join(warnings)
    return False, ""

# ══════════════════════════════════════════════════════
#  🔗  CORRELACIÓN ENTRE PARES
# ══════════════════════════════════════════════════════
def get_correlation_confluence(symbol, direction):
    """
    Comprueba si otros pares han dado señal en la misma dirección
    en los últimos 30 minutos. Cada coincidencia suma una confluencia.
    """
    from datetime import timedelta
    now = datetime.now(timezone.utc)
    confluences = []
    with recent_directions_lock:
        for sym, data in recent_directions.items():
            if sym == symbol:
                continue
            age = (now - data["time"]).total_seconds() / 60
            if age <= 30 and data["direction"] == direction:
                confluences.append(f"🔗 Correlacion con {sym} ({direction})")
    return confluences

def register_signal_direction(symbol, direction):
    """Guarda la direccion de la ultima senal de un par"""
    with recent_directions_lock:
        recent_directions[symbol] = {
            "direction": direction,
            "time": datetime.now(timezone.utc)
        }

# ══════════════════════════════════════════════════════
#  🗄️  BASE DE DATOS
# ══════════════════════════════════════════════════════
def init_db():
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, symbol TEXT, timeframe TEXT, direction TEXT,
            entry_price REAL, sl REAL, tp1 REAL, tp2 REAL,
            rr_tp1 REAL, rr_tp2 REAL, confluences INTEGER,
            confluence_detail TEXT, strength TEXT,
            result TEXT DEFAULT 'PENDING',
            session_id INTEGER DEFAULT 1,
            message_id TEXT DEFAULT NULL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT,
            ended_at TEXT,
            label TEXT
        )
    """)
    c.execute("INSERT OR IGNORE INTO sessions (id, started_at, label) VALUES (1, datetime('now'), 'Sesion 1')")
    conn.commit(); conn.close()
    log.info("✅ Base de datos lista.")

def get_current_session():
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("SELECT id, label FROM sessions ORDER BY id DESC LIMIT 1")
    row = c.fetchone(); conn.close()
    return row if row else (1, "Sesion 1")

def reset_session():
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("UPDATE sessions SET ended_at=datetime('now') WHERE ended_at IS NULL")
    c.execute("SELECT COUNT(*) FROM sessions")
    n = c.fetchone()[0]
    label = f"Sesion {n+1}"
    c.execute("INSERT INTO sessions (started_at, label) VALUES (datetime('now'), ?)", (label,))
    conn.commit(); conn.close()
    return label

def update_signal_result(message_id, result):
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("UPDATE signals SET result=? WHERE message_id=?", (result, str(message_id)))
    affected = conn.total_changes
    conn.commit(); conn.close()
    return affected > 0

def save_message_id(signal_id, message_id):
    with db_lock:
        conn = sqlite3.connect("signals.db")
        conn.cursor().execute("UPDATE signals SET message_id=? WHERE id=?", (str(message_id), signal_id))
        conn.commit(); conn.close()

def get_session_stats(session_id=None):
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    if session_id:
        c.execute("SELECT * FROM signals WHERE session_id=?", (session_id,))
    else:
        sess_id, _ = get_current_session()
        c.execute("SELECT * FROM signals WHERE session_id=?", (sess_id,))
    rows = c.fetchall(); conn.close()
    total     = len(rows)
    apuestas  = sum(1 for r in rows if r[14] in ("APUESTA","TP1","TP2","SL"))
    aciertos  = sum(1 for r in rows if r[14] in ("TP1","TP2"))
    fallos    = sum(1 for r in rows if r[14] == "SL")
    pendientes= sum(1 for r in rows if r[14] == "PENDING")
    wr = round(aciertos / apuestas * 100, 1) if apuestas > 0 else 0
    return {"total": total, "apuestas": apuestas, "aciertos": aciertos,
            "fallos": fallos, "pendientes": pendientes, "win_rate": wr}

def get_all_sessions_stats():
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("SELECT id, label, started_at, ended_at FROM sessions ORDER BY id")
    sessions = c.fetchall()
    result = []
    for sess in sessions:
        sid, label, started, ended = sess
        c.execute("SELECT result FROM signals WHERE session_id=?", (sid,))
        rows = c.fetchall()
        ap  = sum(1 for r in rows if r[0] in ("APUESTA","TP1","TP2","SL"))
        ac  = sum(1 for r in rows if r[0] in ("TP1","TP2"))
        fa  = sum(1 for r in rows if r[0] == "SL")
        wr  = round(ac/ap*100,1) if ap > 0 else 0
        result.append({"id":sid,"label":label,"started":started,"ended":ended,
                       "apuestas":ap,"aciertos":ac,"fallos":fa,"win_rate":wr})
    conn.close()
    return result

def save_signal(sig):
    with db_lock:
        sess_id, _ = get_current_session()
        conn = sqlite3.connect("signals.db")
        c = conn.cursor()
        c.execute("""
            INSERT INTO signals (timestamp,symbol,timeframe,direction,entry_price,
            sl,tp1,tp2,rr_tp1,rr_tp2,confluences,confluence_detail,strength,session_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (sig["timestamp"], sig["symbol"], sig["timeframe"], sig["direction"],
              sig["entry_price"], sig["sl"], sig["tp1"], sig["tp2"],
              sig["rr_tp1"], sig["rr_tp2"], sig["confluences"],
              json.dumps(sig["confluence_detail"]), sig["strength"], sess_id))
        signal_id = c.lastrowid
        conn.commit(); conn.close()
        return signal_id

def get_stats(symbol=None):
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("SELECT * FROM signals WHERE symbol=?" if symbol else "SELECT * FROM signals",
              (symbol,) if symbol else ())
    rows = c.fetchall(); conn.close()
    total = len(rows)
    if total == 0:
        return {"total": 0, "win_rate_tp1": 0, "win_rate_tp2": 0}
    return {
        "total": total,
        "win_rate_tp1": round(sum(1 for r in rows if r[14] in ("TP1","TP2")) / total * 100, 1),
        "win_rate_tp2": round(sum(1 for r in rows if r[14] == "TP2") / total * 100, 1)
    }

# ══════════════════════════════════════════════════════
#  📡  DATOS DE MERCADO
# ══════════════════════════════════════════════════════
def fetch_candles(symbol, interval, limit=300):
    try:
        r = requests.get("https://api.bybit.com/v5/market/kline",
            params={"category": "linear", "symbol": symbol,
                    "interval": interval, "limit": limit}, timeout=10)
        data = r.json()
        if data["retCode"] != 0: return None
        df = pd.DataFrame(data["result"]["list"],
            columns=["timestamp","open","high","low","close","volume","turnover"])
        df = df.astype({"timestamp":"int64","open":"float64","high":"float64",
                        "low":"float64","close":"float64","volume":"float64"})
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        return df.sort_values("timestamp").reset_index(drop=True)
    except Exception as e:
        log.error(f"Error fetch {symbol}: {e}"); return None

def fetch_funding(symbol):
    try:
        r = requests.get("https://api.bybit.com/v5/market/tickers",
            params={"category": "linear", "symbol": symbol}, timeout=10)
        items = r.json()["result"]["list"]
        if items: return float(items[0].get("fundingRate", 0))
    except: pass
    return None

# ══════════════════════════════════════════════════════
#  🔷  BLOCKSCOUT — DATOS ON-CHAIN (solo ETHUSDT)
# ══════════════════════════════════════════════════════

# Wallets conocidas de exchanges (Ethereum mainnet)
EXCHANGE_WALLETS = {
    "0x3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE": "Binance",
    "0xD551234Ae421e3BCBA99A0Da6d736074f22192FF": "Binance",
    "0x564286362092D8e7936f0549571a803B203aAceD": "Binance",
    "0xfE9e8709d3215310075d67E3ed32A380CCf451C8": "Binance",
    "0x0681d8Db095565FE8A346fA0277bFfdE9C0eDBBF": "Binance",
    "0xA910f92ACdAf488fa6eF02174fb86208Ad7722ba": "Kraken",
    "0xE853c56864A2ebe4576a807D26Fdc4A0adA51919": "Kraken",
    "0x267be1C1D684F78cb4F6a176C4911b741E4Ffdc0": "Kraken",
    "0xAe2D4617c862309A3d75A0fFB358c7a5009c673F": "Coinbase",
    "0x02466E547BFDAb679fC49e96bBfc62B9747D997C": "Coinbase",
    "0x77696bb39917C91A0c3908D577d5e322095425cA": "Coinbase",
    "0x236F9F97e0E62388479bf9E5BA4889e46B0273C3": "OKX",
    "0x98EC059Dc3aDFBdd63429454aeB0c990FBA4A128": "OKX",
    "0x6Fc82a5fe25A5cDb58bc74600A40A69C065263f8": "Gemini",
}

BLOCKSCOUT_BASE = "https://eth.blockscout.com/api/v2"

def fetch_blockscout_onchain(symbol):
    """
    Consulta Blockscout para ETHUSDT:
    - Detecta transferencias grandes de ballenas (>100 ETH)
    - Detecta flujos hacia exchanges conocidos
    Devuelve dict con confluencias detectadas.
    """
    if symbol != "ETHUSDT":
        return {"whale_activity": False, "exchange_inflow": False,
                "exchange_outflow": False, "whale_count": 0, "exchange_name": None}

    result = {"whale_activity": False, "exchange_inflow": False,
              "exchange_outflow": False, "whale_count": 0, "exchange_name": None}
    try:
        # Consultar transferencias recientes de ETH (últimas 50)
        r = requests.get(
            f"{BLOCKSCOUT_BASE}/transactions",
            params={"filter": "validated", "type": "coin_transfer"},
            timeout=8
        )
        if r.status_code != 200:
            return result

        txs = r.json().get("items", [])
        whale_count = 0
        exchange_flows = []

        for tx in txs[:50]:
            try:
                value_wei = int(tx.get("value", "0"))
                value_eth = value_wei / 1e18

                to_addr   = (tx.get("to",   {}) or {}).get("hash", "").lower()
                from_addr = (tx.get("from", {}) or {}).get("hash", "").lower()

                # Ballenas: transferencias > 100 ETH
                if value_eth >= 100:
                    whale_count += 1

                # Flujos hacia exchanges (inflow = posible venta)
                for addr, name in EXCHANGE_WALLETS.items():
                    if to_addr == addr.lower():
                        exchange_flows.append(("inflow", name, value_eth))
                    if from_addr == addr.lower():
                        exchange_flows.append(("outflow", name, value_eth))

            except Exception:
                continue

        # Resultado
        if whale_count >= 2:
            result["whale_activity"] = True
            result["whale_count"]    = whale_count

        inflows  = [f for f in exchange_flows if f[0] == "inflow"  and f[2] >= 10]
        outflows = [f for f in exchange_flows if f[0] == "outflow" and f[2] >= 10]

        if inflows:
            result["exchange_inflow"] = True
            result["exchange_name"]   = inflows[0][1]
        if outflows:
            result["exchange_outflow"] = True
            if not result["exchange_name"]:
                result["exchange_name"] = outflows[0][1]

        log.info(f"🔷 Blockscout — ballenas: {whale_count} | "
                 f"inflow: {result['exchange_inflow']} | outflow: {result['exchange_outflow']}")

    except Exception as e:
        log.warning(f"⚠️ Blockscout no disponible: {e}")

    return result

# ══════════════════════════════════════════════════════
#  🔭  FILTRO DE TENDENCIA EN TIMEFRAME MAYOR
# ══════════════════════════════════════════════════════
def get_higher_tf_trend(symbol, timeframe):
    higher_tf_map = {"15": "60", "60": "240"}
    higher_tf = higher_tf_map.get(timeframe)
    if not higher_tf:
        return None

    try:
        df = fetch_candles(symbol, higher_tf, limit=100)
        if df is None or len(df) < 50:
            return None

        ema9  = df["close"].ewm(span=9,  adjust=False).mean().iloc[-1]
        ema21 = df["close"].ewm(span=21, adjust=False).mean().iloc[-1]
        ema50 = df["close"].ewm(span=50, adjust=False).mean().iloc[-1]
        price = df["close"].iloc[-1]

        if ema9 > ema21 > ema50 and price > ema50:
            return "BULL"
        elif ema9 < ema21 < ema50 and price < ema50:
            return "BEAR"
        else:
            return "NEUTRAL"
    except Exception as e:
        log.error(f"Error get_higher_tf_trend {symbol} {timeframe}: {e}")
        return None

# ══════════════════════════════════════════════════════
#  📊  INDICADORES
# ══════════════════════════════════════════════════════
def compute_indicators(df):
    df.ta.rsi(length=14, append=True)
    df.ta.macd(fast=12, slow=26, signal=9, append=True)
    df.ta.ema(length=9,   append=True)
    df.ta.ema(length=21,  append=True)
    df.ta.ema(length=50,  append=True)
    df.ta.ema(length=200, append=True)
    df.ta.atr(length=14,  append=True)
    df.ta.bbands(length=20, append=True)
    df.ta.adx(length=14,  append=True)
    df["vol_sma20"] = df["volume"].rolling(20).mean()
    return df

# ══════════════════════════════════════════════════════
#  🕯️  PATRONES DE VELA
# ══════════════════════════════════════════════════════
def detect_candles(df):
    p = {
        "bull_engulf": False, "bear_engulf": False,
        "hammer": False, "shooting_star": False,
        "pin_bull": False, "pin_bear": False,
        "morning_star": False, "evening_star": False,
        "three_white": False, "three_black": False,
        "bull_harami": False, "bear_harami": False,
        "bull_marubozu": False, "bear_marubozu": False,
        "tweezer_bottom": False, "tweezer_top": False,
        "bull_harami_cross": False, "bear_harami_cross": False,
    }
    if len(df) < 5: return p
    c     = df.iloc[-1]
    prev  = df.iloc[-2]
    prev2 = df.iloc[-3]
    prev3 = df.iloc[-4]

    body  = abs(c["close"] - c["open"])
    rng   = c["high"] - c["low"]
    upper = c["high"] - max(c["close"], c["open"])
    lower = min(c["close"], c["open"]) - c["low"]

    prev_body  = abs(prev["close"]  - prev["open"])
    prev2_body = abs(prev2["close"] - prev2["open"])
    prev3_body = abs(prev3["close"] - prev3["open"])

    if rng > 0:
        if (prev["close"] < prev["open"] and c["close"] > c["open"] and
                c["open"] <= prev["close"] and c["close"] >= prev["open"] and
                body > prev_body):
            p["bull_engulf"] = True
        if (prev["close"] > prev["open"] and c["close"] < c["open"] and
                c["open"] >= prev["close"] and c["close"] <= prev["open"] and
                body > prev_body):
            p["bear_engulf"] = True

        if lower > body * 2.5 and upper < body * 0.5 and body > 0:
            p["hammer"] = True
        if upper > body * 2.5 and lower < body * 0.5 and body > 0:
            p["shooting_star"] = True

        if lower > rng * 0.65 and body < rng * 0.25:
            p["pin_bull"] = True
        if upper > rng * 0.65 and body < rng * 0.25:
            p["pin_bear"] = True

        if (c["close"] > c["open"] and
                upper < body * 0.05 and lower < body * 0.05 and body > 0):
            p["bull_marubozu"] = True
        if (c["close"] < c["open"] and
                upper < body * 0.05 and lower < body * 0.05 and body > 0):
            p["bear_marubozu"] = True

        if (prev["close"] < prev["open"] and c["close"] > c["open"] and
                c["open"] > prev["close"] and c["close"] < prev["open"] and
                body < prev_body * 0.6):
            p["bull_harami"] = True
        if (prev["close"] > prev["open"] and c["close"] < c["open"] and
                c["open"] < prev["close"] and c["close"] > prev["open"] and
                body < prev_body * 0.6):
            p["bear_harami"] = True

        if (prev["close"] < prev["open"] and body < rng * 0.1 and
                c["open"] > prev["close"] and c["close"] < prev["open"]):
            p["bull_harami_cross"] = True
        if (prev["close"] > prev["open"] and body < rng * 0.1 and
                c["open"] < prev["close"] and c["close"] > prev["open"]):
            p["bear_harami_cross"] = True

    if (prev["close"] < prev["open"] and c["close"] > c["open"] and
            abs(prev["low"] - c["low"]) / max(prev["low"], c["low"]) < 0.001):
        p["tweezer_bottom"] = True
    if (prev["close"] > prev["open"] and c["close"] < c["open"] and
            abs(prev["high"] - c["high"]) / max(prev["high"], c["high"]) < 0.001):
        p["tweezer_top"] = True

    if (prev2["close"] > prev2["open"] and prev["close"] > prev["open"] and
            c["close"] > c["open"] and
            prev["close"] > prev2["close"] and c["close"] > prev["close"] and
            prev2_body > 0 and prev_body > prev2_body * 0.7 and body > prev_body * 0.7):
        p["three_white"] = True

    if (prev2["close"] < prev2["open"] and prev["close"] < prev["open"] and
            c["close"] < c["open"] and
            prev["close"] < prev2["close"] and c["close"] < prev["close"] and
            prev2_body > 0 and prev_body > prev2_body * 0.7 and body > prev_body * 0.7):
        p["three_black"] = True

    if (prev2["close"] < prev2["open"] and prev_body < prev2_body * 0.5 and
            c["close"] > c["open"] and c["close"] > (prev2["open"] + prev2["close"]) / 2):
        p["morning_star"] = True
    if (prev2["close"] > prev2["open"] and prev_body < prev2_body * 0.5 and
            c["close"] < c["open"] and c["close"] < (prev2["open"] + prev2["close"]) / 2):
        p["evening_star"] = True

    return p

# ══════════════════════════════════════════════════════
#  🧱  ORDER BLOCKS
# ══════════════════════════════════════════════════════
def detect_ob(df):
    result = {"bull_ob": None, "bear_ob": None}
    if len(df) < 50: return result
    sub   = df.tail(50).reset_index(drop=True)
    price = sub.iloc[-1]["close"]
    for i in range(len(sub) - 4, 3, -1):
        c     = sub.iloc[i]
        next1 = sub.iloc[i + 1]
        if c["close"] > c["open"]:
            move = next1["open"] - next1["close"]
            if move > abs(c["close"] - c["open"]) * 1.2:
                if c["low"] <= price <= c["high"] * 1.003:
                    result["bear_ob"] = {"high": c["high"], "low": c["low"]}
                    break
    for i in range(len(sub) - 4, 3, -1):
        c     = sub.iloc[i]
        next1 = sub.iloc[i + 1]
        if c["close"] < c["open"]:
            move = next1["close"] - next1["open"]
            if move > abs(c["open"] - c["close"]) * 1.2:
                if c["low"] * 0.997 <= price <= c["high"]:
                    result["bull_ob"] = {"high": c["high"], "low": c["low"]}
                    break
    return result

# ══════════════════════════════════════════════════════
#  ⚡  IMBALANCES / FAIR VALUE GAPS (FVG)
# ══════════════════════════════════════════════════════
def detect_fvg(df):
    result = {"bull_fvg": None, "bear_fvg": None}
    if len(df) < 10: return result
    price = df.iloc[-1]["close"]
    sub   = df.tail(50).reset_index(drop=True)
    for i in range(len(sub) - 1, 1, -1):
        low_cur  = sub.iloc[i]["low"]
        high_old = sub.iloc[i - 2]["high"]
        if low_cur > high_old:
            fvg_top = low_cur; fvg_bot = high_old
            if fvg_bot <= price <= fvg_top * 1.003:
                result["bear_fvg"] = {"top": fvg_top, "bot": fvg_bot}; break
    for i in range(len(sub) - 1, 1, -1):
        high_cur = sub.iloc[i]["high"]
        low_old  = sub.iloc[i - 2]["low"]
        if high_cur < low_old:
            fvg_top = low_old; fvg_bot = high_cur
            if fvg_bot * 0.997 <= price <= fvg_top:
                result["bull_fvg"] = {"top": fvg_top, "bot": fvg_bot}; break
    return result

# ══════════════════════════════════════════════════════
#  📏  SOPORTES Y RESISTENCIAS
# ══════════════════════════════════════════════════════
def detect_sr(df):
    result = {"resistance": None, "support": None,
              "all_resistances": [], "all_supports": []}
    if len(df) < 100: return result
    sub   = df.tail(100)
    price = df.iloc[-1]["close"]
    levels = []
    for i in range(3, len(sub) - 3):
        h = sub.iloc[i]["high"]
        l = sub.iloc[i]["low"]
        if (h > sub.iloc[i-1]["high"] and h > sub.iloc[i-2]["high"] and
                h > sub.iloc[i-3]["high"] and h > sub.iloc[i+1]["high"] and
                h > sub.iloc[i+2]["high"] and h > sub.iloc[i+3]["high"]):
            levels.append(("R", h))
        if (l < sub.iloc[i-1]["low"] and l < sub.iloc[i-2]["low"] and
                l < sub.iloc[i-3]["low"] and l < sub.iloc[i+1]["low"] and
                l < sub.iloc[i+2]["low"] and l < sub.iloc[i+3]["low"]):
            levels.append(("S", l))
    resistances = sorted([v for t, v in levels if t == "R" and v > price * 1.002])
    supports    = sorted([v for t, v in levels if t == "S" and v < price * 0.998], reverse=True)
    result["all_resistances"] = resistances[:5]
    result["all_supports"]    = supports[:5]
    if resistances: result["resistance"] = resistances[0]
    if supports:    result["support"]    = supports[0]
    return result

# ══════════════════════════════════════════════════════
#  🌀  FIBONACCI
# ══════════════════════════════════════════════════════
def compute_fib(df):
    sub = df.tail(100)
    sh  = sub["high"].max()
    sl  = sub["low"].min()
    d   = sh - sl
    price = df.iloc[-1]["close"]
    levels = {
        "0.0":   sh,   "0.236": sh - d * 0.236,
        "0.382": sh - d * 0.382, "0.5":   sh - d * 0.5,
        "0.618": sh - d * 0.618, "0.786": sh - d * 0.786,
        "1.0":   sl,   "1.272": sl - d * 0.272,
        "1.618": sl - d * 0.618,
    }
    near = next((k for k, v in levels.items() if abs(price - v) / price < 0.004), None)
    return {"levels": levels, "near_level": near}

# ══════════════════════════════════════════════════════
#  📦  VOLUME PROFILE
# ══════════════════════════════════════════════════════
def compute_vp(df, bins=24):
    if len(df) < 20: return {"poc": None, "vah": None, "val": None}
    lo = df["low"].min(); hi = df["high"].max()
    bin_sz = (hi - lo) / bins
    if bin_sz == 0: return {"poc": None, "vah": None, "val": None}
    profile = {}
    for _, row in df.iterrows():
        n   = max(1, int((row["high"] - row["low"]) / bin_sz))
        vpb = row["volume"] / n
        for b in range(n + 1):
            pl = row["low"] + b * bin_sz
            bk = round(lo + int((pl - lo) / bin_sz) * bin_sz, 2)
            profile[bk] = profile.get(bk, 0) + vpb
    if not profile: return {"poc": None, "vah": None, "val": None}
    sp    = sorted(profile.items(), key=lambda x: x[1], reverse=True)
    poc   = sp[0][0]
    total = sum(profile.values()); acc = 0; va = []
    for bk, v in sp:
        va.append(bk); acc += v
        if acc >= total * 0.7: break
    return {"poc": poc, "vah": max(va) if va else None, "val": min(va) if va else None}

# ══════════════════════════════════════════════════════
#  🎯  SL INTELIGENTE
# ══════════════════════════════════════════════════════
def smart_sl(df, direction, price, atr, ob, sr):
    buffer = atr * 0.5
    if direction == "LONG":
        candidates = []
        if ob["bull_ob"]:
            candidates.append(ob["bull_ob"]["low"] - buffer)
        if sr["support"]:
            candidates.append(sr["support"] - buffer)
        candidates.append(df.tail(15)["low"].min() - buffer)
        candidates.append(price - atr * 2.0)
        valid = [c for c in candidates if c < price * 0.998]
        sl = max(valid) if valid else price - atr * 2.0
        sl = max(sl, price * 0.95)
    else:
        candidates = []
        if ob["bear_ob"]:
            candidates.append(ob["bear_ob"]["high"] + buffer)
        if sr["resistance"]:
            candidates.append(sr["resistance"] + buffer)
        candidates.append(df.tail(15)["high"].max() + buffer)
        candidates.append(price + atr * 2.0)
        valid = [c for c in candidates if c > price * 1.002]
        sl = min(valid) if valid else price + atr * 2.0
        sl = min(sl, price * 1.05)
    return round(sl, 4)

# ══════════════════════════════════════════════════════
#  🏆  TP INTELIGENTE
# ══════════════════════════════════════════════════════
def smart_tp(direction, price, sl, sr, fib, vp):
    risk = abs(price - sl)
    if direction == "LONG":
        tp1_candidates = [price + risk * 2.0]
        if sr["resistance"] and sr["resistance"] > price + risk * 1.5:
            tp1_candidates.append(sr["resistance"])
        tp1 = min(tp1_candidates)
        tp2_candidates = [price + risk * 4.0]
        if fib["levels"].get("1.272") and fib["levels"]["1.272"] > tp1:
            tp2_candidates.append(fib["levels"]["1.272"])
        if fib["levels"].get("1.618") and fib["levels"]["1.618"] > tp1:
            tp2_candidates.append(fib["levels"]["1.618"])
        far = [r for r in sr["all_resistances"] if r > tp1 * 1.01]
        if far: tp2_candidates.append(far[-1])
        tp2 = max(tp2_candidates)
    else:
        tp1_candidates = [price - risk * 2.0]
        if sr["support"] and sr["support"] < price - risk * 1.5:
            tp1_candidates.append(sr["support"])
        tp1 = max(tp1_candidates)
        tp2_candidates = [price - risk * 4.0]
        if fib["levels"].get("1.272") and fib["levels"]["1.272"] < tp1:
            tp2_candidates.append(fib["levels"]["1.272"])
        if fib["levels"].get("1.618") and fib["levels"]["1.618"] < tp1:
            tp2_candidates.append(fib["levels"]["1.618"])
        far = [s for s in sr["all_supports"] if s < tp1 * 0.99]
        if far: tp2_candidates.append(far[-1])
        tp2 = min(tp2_candidates)
    return round(tp1, 4), round(tp2, 4)

# ══════════════════════════════════════════════════════
#  🧠  ANÁLISIS PRINCIPAL
# ══════════════════════════════════════════════════════
def analyze(df, symbol, timeframe):
    if len(df) < 100: return None
    df  = compute_indicators(df)
    last  = df.iloc[-1]; prev = df.iloc[-2]; price = last["close"]

    rsi     = last.get("RSI_14")
    macdh   = last.get("MACDh_12_26_9")
    macdh_p = prev.get("MACDh_12_26_9")
    ema9    = last.get("EMA_9")
    ema21   = last.get("EMA_21")
    ema50   = last.get("EMA_50")
    ema200  = last.get("EMA_200")
    atr     = last.get("ATRr_14")
    vol     = last.get("volume")
    vol20   = last.get("vol_sma20")
    bb_up   = last.get("BBU_20_2.0")
    bb_lo   = last.get("BBL_20_2.0")
    adx     = last.get("ADX_14")

    if any(v is None or (isinstance(v, float) and np.isnan(v))
           for v in [rsi, macdh, ema9, ema21, ema50, atr]):
        return None

    if adx and not np.isnan(adx) and adx < 20:
        log.info(f"  📊 ADX {adx:.1f} < 20 — mercado lateral, descartado")
        return None

    htf_map = {"15": "60", "60": "240"}
    if timeframe in htf_map:
        higher_tf = htf_map[timeframe]
        df_htf = fetch_candles(symbol, higher_tf, limit=100)
        if df_htf is not None and len(df_htf) >= 50:
            df_htf = compute_indicators(df_htf)
            htf_last = df_htf.iloc[-1]
            htf_ema9  = htf_last.get("EMA_9")
            htf_ema21 = htf_last.get("EMA_21")
            htf_ema50 = htf_last.get("EMA_50")
            htf_price = htf_last["close"]
            if htf_ema9 and htf_ema21 and htf_ema50:
                htf_bull = htf_ema9 > htf_ema21 and htf_price > htf_ema50
                htf_bear = htf_ema9 < htf_ema21 and htf_price < htf_ema50
            else:
                htf_bull = htf_bear = False
        else:
            htf_bull = htf_bear = False
    else:
        htf_bull = htf_bear = True

    rsi_vals   = df["RSI_14"].tail(10).dropna().values
    close_vals = df["close"].tail(10).values
    macd_vals  = df["MACDh_12_26_9"].tail(10).dropna().values

    # Divergencias RSI
    bull_div = (len(rsi_vals) >= 5 and
                close_vals[-1] < close_vals[-5] and rsi_vals[-1] > rsi_vals[-5])
    bear_div = (len(rsi_vals) >= 5 and
                close_vals[-1] > close_vals[-5] and rsi_vals[-1] < rsi_vals[-5])

    # Divergencias MACD
    macd_bull_div = (len(macd_vals) >= 5 and len(close_vals) >= 5 and
                     close_vals[-1] < close_vals[-5] and macd_vals[-1] > macd_vals[-5])
    macd_bear_div = (len(macd_vals) >= 5 and len(close_vals) >= 5 and
                     close_vals[-1] > close_vals[-5] and macd_vals[-1] < macd_vals[-5])

    # Niveles psicológicos — múltiplos redondos según el precio
    def near_psych_level(p):
        if p > 10000:   step = 5000
        elif p > 1000:  step = 1000
        elif p > 100:   step = 100
        elif p > 10:    step = 10
        else:           step = 1
        nearest = round(p / step) * step
        return abs(p - nearest) / p < 0.003, nearest

    psych_hit, psych_level = near_psych_level(price)

    can     = detect_candles(df)
    ob      = detect_ob(df)
    sr      = detect_sr(df)
    fib     = compute_fib(df)
    vp      = compute_vp(df)
    funding = fetch_funding(symbol)
    fvg     = detect_fvg(df)
    onchain = fetch_blockscout_onchain(symbol)  # 🔷 datos on-chain ETH
    fg      = fetch_fear_greed() if symbol == "BTCUSDT" else None  # 😱 solo BTC
    deribit = fetch_deribit_options(symbol) if symbol in ("BTCUSDT","ETHUSDT") else None

    # 🎭 Detección de manipulación
    manipulation, manip_desc = detect_manipulation(df)

    vol_spike  = bool(vol20 and vol > vol20 * 1.8)
    vol_climax = bool(vol20 and vol > vol20 * 3.0)

    trend_bull = (ema9 > ema21 > ema50) and (price > ema50)
    trend_bear = (ema9 < ema21 < ema50) and (price < ema50)
    if ema200 and not np.isnan(ema200):
        trend_bull = trend_bull and price > ema200
        trend_bear = trend_bear and price < ema200

    near_bb_lo = bool(bb_lo and abs(price - bb_lo) / price < 0.003)
    near_bb_up = bool(bb_up and abs(price - bb_up) / price < 0.003)

    long_c = []
    if trend_bull:            long_c.append("✅ Tendencia alcista EMA9>21>50")
    if rsi < 35:              long_c.append(f"📉 RSI {rsi:.1f} sobrevendido")
    elif rsi < 45:            long_c.append(f"📊 RSI {rsi:.1f} zona de compra")
    if macdh > 0 and macdh_p <= 0:  long_c.append("🔀 MACD cruce alcista")
    elif macdh > 0 and macdh > prev.get("MACDh_12_26_9", 0):
                              long_c.append("📈 MACD histograma creciendo")
    if bull_div:              long_c.append("🔁 Divergencia alcista RSI")
    if macd_bull_div:         long_c.append("🔁 Divergencia alcista MACD")
    if psych_hit:             long_c.append(f"🔢 Nivel psicológico {psych_level:,.0f}")
    if ob["bull_ob"]:         long_c.append(f"🧱 Order Block alcista {ob['bull_ob']['low']:.2f}-{ob['bull_ob']['high']:.2f}")
    if sr["support"] and abs(price - sr["support"]) / price < 0.006:
                              long_c.append(f"🛡️ Soporte clave {sr['support']:.2f}")
    if fib["near_level"] in ("0.618","0.786","1.0"):
                              long_c.append(f"🌀 Fibonacci {fib['near_level']}")
    if can["morning_star"]:        long_c.append("🌅 Morning Star")
    elif can["three_white"]:       long_c.append("🕯️ Three White Soldiers")
    elif can["bull_engulf"]:       long_c.append("🕯️ Engulfing alcista")
    elif can["bull_marubozu"]:     long_c.append("💪 Marubozu alcista")
    elif can["tweezer_bottom"]:    long_c.append("🔧 Tweezer Bottom")
    elif can["hammer"]:            long_c.append("🔨 Hammer")
    elif can["pin_bull"]:          long_c.append("📌 Pin Bar alcista")
    elif can["bull_harami_cross"]: long_c.append("✨ Harami Cross alcista")
    elif can["bull_harami"]:       long_c.append("🔀 Harami alcista")
    if near_bb_lo:            long_c.append("📊 Precio en banda inferior BB")
    if fvg["bull_fvg"]:       long_c.append(f"⚡ Imbalance alcista {fvg['bull_fvg']['bot']:.2f}-{fvg['bull_fvg']['top']:.2f}")
    if vol_climax:            long_c.append("🔥 Volumen climático")
    elif vol_spike:           long_c.append("📊 Spike de volumen")
    if funding and funding < -0.0005:
                              long_c.append(f"💰 Funding negativo {funding*100:.4f}%")
    if vp["val"] and abs(price - vp["val"]) / price < 0.005:
                              long_c.append(f"🎯 Precio en VAL {vp['val']:.2f}")
    # 🔷 Blockscout on-chain (solo ETH)
    if onchain["whale_activity"]:
                              long_c.append(f"🐋 Ballenas activas — {onchain['whale_count']} transfers >100 ETH")
    if onchain["exchange_outflow"]:
                              long_c.append(f"🏦 Salida de {onchain['exchange_name']} — presión compradora")
    # 😱 Fear & Greed (solo BTC)
    if fg and fg["value"] <= 25:
                              long_c.append(f"😱 Fear & Greed: {fg['value']} ({fg['label']}) — miedo extremo")
    # 📊 Deribit opciones
    if deribit and deribit["ratio"] >= 1.2:
                              long_c.append(f"📊 Deribit puts/calls: {deribit['ratio']} — posible suelo")

    short_c = []
    if trend_bear:            short_c.append("✅ Tendencia bajista EMA9<21<50")
    if rsi > 65:              short_c.append(f"📈 RSI {rsi:.1f} sobrecomprado")
    elif rsi > 55:            short_c.append(f"📊 RSI {rsi:.1f} zona de venta")
    if macdh < 0 and macdh_p >= 0:  short_c.append("🔀 MACD cruce bajista")
    elif macdh < 0 and macdh < prev.get("MACDh_12_26_9", 0):
                              short_c.append("📉 MACD histograma cayendo")
    if bear_div:              short_c.append("🔁 Divergencia bajista RSI")
    if macd_bear_div:         short_c.append("🔁 Divergencia bajista MACD")
    if psych_hit:             short_c.append(f"🔢 Nivel psicológico {psych_level:,.0f}")
    if ob["bear_ob"]:         short_c.append(f"🧱 Order Block bajista {ob['bear_ob']['low']:.2f}-{ob['bear_ob']['high']:.2f}")
    if sr["resistance"] and abs(price - sr["resistance"]) / price < 0.006:
                              short_c.append(f"🚧 Resistencia clave {sr['resistance']:.2f}")
    if fib["near_level"] in ("0.0","0.236","0.382"):
                              short_c.append(f"🌀 Fibonacci {fib['near_level']}")
    if can["evening_star"]:        short_c.append("🌆 Evening Star")
    elif can["three_black"]:       short_c.append("🕯️ Three Black Crows")
    elif can["bear_engulf"]:       short_c.append("🕯️ Engulfing bajista")
    elif can["bear_marubozu"]:     short_c.append("💪 Marubozu bajista")
    elif can["tweezer_top"]:       short_c.append("🔧 Tweezer Top")
    elif can["shooting_star"]:     short_c.append("💫 Shooting Star")
    elif can["pin_bear"]:          short_c.append("📌 Pin Bar bajista")
    elif can["bear_harami_cross"]: short_c.append("✨ Harami Cross bajista")
    elif can["bear_harami"]:       short_c.append("🔀 Harami bajista")
    if near_bb_up:            short_c.append("📊 Precio en banda superior BB")
    if fvg["bear_fvg"]:       short_c.append(f"⚡ Imbalance bajista {fvg['bear_fvg']['bot']:.2f}-{fvg['bear_fvg']['top']:.2f}")
    if vol_climax:            short_c.append("🔥 Volumen climático")
    elif vol_spike:           short_c.append("📊 Spike de volumen")
    if funding and funding > 0.0005:
                              short_c.append(f"💰 Funding positivo {funding*100:.4f}%")
    if vp["vah"] and abs(price - vp["vah"]) / price < 0.005:
                              short_c.append(f"🎯 Precio en VAH {vp['vah']:.2f}")
    # 🔷 Blockscout on-chain (solo ETH)
    if onchain["whale_activity"]:
                              short_c.append(f"🐋 Ballenas activas — {onchain['whale_count']} transfers >100 ETH")
    if onchain["exchange_inflow"]:
                              short_c.append(f"🏦 Flujo hacia {onchain['exchange_name']} — presión vendedora")
    # 😱 Fear & Greed (solo BTC)
    if fg and fg["value"] >= 75:
                              short_c.append(f"🤑 Fear & Greed: {fg['value']} ({fg['label']}) — euforia extrema")
    # 📊 Deribit opciones
    if deribit and deribit["ratio"] <= 0.8:
                              short_c.append(f"📊 Deribit puts/calls: {deribit['ratio']} — presión bajista")

    if timeframe == "15":
        min_conf = 7
    elif timeframe == "60":
        min_conf = 5
    else:
        min_conf = 4

    if len(long_c) >= len(short_c) and len(long_c) >= min_conf:
        direction, confluences = "LONG", long_c
    elif len(short_c) > len(long_c) and len(short_c) >= min_conf:
        direction, confluences = "SHORT", short_c
    else:
        log.info(f"  📈 LONG {len(long_c)} | 📉 SHORT {len(short_c)} — sin señal (min {min_conf} para {timeframe}m)")
        return None

    # 🔗 Correlación entre pares — añadir confluencias extra
    corr = get_correlation_confluence(symbol, direction)
    confluences.extend(corr)
    if corr:
        log.info(f"🔗 Correlacion detectada: {corr}")

    if timeframe in ("15", "60"):
        if direction == "LONG" and htf_bear:
            log.info(f"  🚫 LONG bloqueado — tendencia bajista en HTF")
            return None
        if direction == "SHORT" and htf_bull:
            log.info(f"  🚫 SHORT bloqueado — tendencia alcista en HTF")
            return None

    higher_trend = get_higher_tf_trend(symbol, timeframe)
    if higher_trend is not None:
        if direction == "LONG" and higher_trend == "BEAR":
            log.info(f"🚫 {symbol} {timeframe}m LONG bloqueado — TF mayor bajista")
            return None
        if direction == "SHORT" and higher_trend == "BULL":
            log.info(f"🚫 {symbol} {timeframe}m SHORT bloqueado — TF mayor alcista")
            return None
        if higher_trend == "NEUTRAL":
            log.info(f"⚠️ {symbol} {timeframe}m — TF mayor neutral, señal con precaucion")
        else:
            log.info(f"✅ {symbol} {timeframe}m {direction} confirmado por TF mayor ({higher_trend})")
        tf_label = "60m" if timeframe == "15" else "240m"
        if higher_trend != "NEUTRAL":
            confluences.append(f"✅ Confirmado por {tf_label} ({higher_trend})")

    sl       = smart_sl(df, direction, price, atr, ob, sr)
    tp1, tp2 = smart_tp(direction, price, sl, sr, fib, vp)
    risk     = abs(price - sl)
    if risk == 0: return None
    rr1 = round(abs(tp1 - price) / risk, 2)
    rr2 = round(abs(tp2 - price) / risk, 2)

    if rr1 < CONFIG["min_rr_tp1"]:
        log.info(f"❌ Descartada — TP1 ratio {rr1:.1f} < {CONFIG['min_rr_tp1']}"); return None
    if rr2 < CONFIG["min_rr_tp2"]:
        log.info(f"❌ Descartada — TP2 ratio {rr2:.1f} < {CONFIG['min_rr_tp2']}"); return None

    n = len(confluences)
    strength = ("🔥 MUY ALTA" if n >= 7 else "⚡ ALTA" if n >= 5 else "◆ MEDIA" if n >= 4 else "▼ BAJA")

    from datetime import timedelta
    spain_offset = timedelta(hours=CONFIG.get("utc_offset", 2))
    spain_time = datetime.now(timezone.utc) + spain_offset

    return {
        "timestamp":         spain_time.strftime("%Y-%m-%d %H:%M (España)"),
        "symbol":            symbol,
        "timeframe":         f"{timeframe}m",
        "direction":         direction,
        "entry_price":       round(price, 4),
        "sl":                sl, "tp1": tp1, "tp2": tp2,
        "rr_tp1":            rr1, "rr_tp2": rr2,
        "confluences":       n,
        "confluence_detail": confluences,
        "strength":          strength,
        "poc":               vp.get("poc"), "vah": vp.get("vah"), "val": vp.get("val"),
        "funding":           funding,
        "fear_greed":        fg,
        "manipulation":      manipulation,
        "manip_desc":        manip_desc,
        "stats":             get_stats(symbol),
        "rsi":               round(rsi, 1),
    }

# ══════════════════════════════════════════════════════
#  🛡️  GESTIÓN DE RIESGO
# ══════════════════════════════════════════════════════
def calculate_risk(sig):
    """
    Calcula tamaño de posición, apalancamiento y pérdida máxima
    según el capital configurado y la fuerza de la señal.
    """
    capital    = CONFIG.get("capital_total", 1000)
    riesgo_pct = CONFIG.get("riesgo_pct", 1.0)
    strength   = sig.get("strength", "")
    entry      = sig["entry_price"]
    sl         = sig["sl"]

    # Apalancamiento variable según fuerza de señal
    if "MUY ALTA" in strength:
        leverage = CONFIG.get("leverage_muy_alta", 10)
    elif "ALTA" in strength:
        leverage = CONFIG.get("leverage_alta", 5)
    elif "MEDIA" in strength:
        leverage = CONFIG.get("leverage_media", 3)
    else:
        leverage = CONFIG.get("leverage_baja", 1)

    # Pérdida máxima permitida en USDT
    max_loss_usdt = round(capital * (riesgo_pct / 100), 2)

    # Distancia al SL en %
    sl_dist_pct = abs(entry - sl) / entry

    # Tamaño de posición = pérdida máxima / distancia SL
    if sl_dist_pct > 0:
        position_size_usdt = round(max_loss_usdt / sl_dist_pct, 2)
    else:
        position_size_usdt = 0

    # Capital necesario con el apalancamiento elegido
    capital_necesario = round(position_size_usdt / leverage, 2)

    return {
        "leverage":           leverage,
        "max_loss_usdt":      max_loss_usdt,
        "riesgo_pct":         riesgo_pct,
        "position_size_usdt": position_size_usdt,
        "capital_necesario":  capital_necesario,
    }

# ══════════════════════════════════════════════════════
#  📨  MENSAJE TELEGRAM
# ══════════════════════════════════════════════════════
def build_message(sig):
    dir_emoji = "🟢 LONG 📈" if sig["direction"] == "LONG" else "🔴 SHORT 📉"
    sl_pct  = abs(sig["entry_price"] - sig["sl"])  / sig["entry_price"] * 100
    tp1_pct = abs(sig["tp1"] - sig["entry_price"]) / sig["entry_price"] * 100
    tp2_pct = abs(sig["tp2"] - sig["entry_price"]) / sig["entry_price"] * 100
    stats   = sig.get("stats", {})
    poc = sig.get("poc"); vah = sig.get("vah"); val = sig.get("val")
    conf    = "\n".join(f"  {c}" for c in sig["confluence_detail"])
    vp_text = ""
    if poc: vp_text += f"  💎 POC: ${poc:,.2f}\n"
    if vah: vp_text += f"  ⬆️  VAH: ${vah:,.2f}\n"
    if val: vp_text += f"  ⬇️  VAL: ${val:,.2f}\n"
    hist = (f"📊 Historial: {stats['total']} señales | TP1: {stats['win_rate_tp1']}% | TP2: {stats['win_rate_tp2']}%"
            if stats.get("total", 0) > 0 else "📊 Sin historial previo")

    risk = calculate_risk(sig)
    vp_or_default = vp_text if vp_text else "  Sin datos\n"

    fg = sig.get("fear_greed")
    fg_line = f"\n😱 Fear & Greed: {fg['value']} — {fg['label']}" if fg else ""

    manip = sig.get("manipulation", False)
    manip_line = f"\n\n⚠️ POSIBLE TRAMPA: {sig.get('manip_desc','')}\n   Opera con precaución extra." if manip else ""

    return f"""🚨 SEÑAL — {sig["symbol"]} {sig["timeframe"]}
⏰ {sig["timestamp"]}

💰 Entrada: ${sig["entry_price"]:,.4f}
{dir_emoji}

━━━━━━━━━━━━━━━━━━━━
🛑 SL:  ${sig["sl"]:,.4f}  (-{sl_pct:.2f}%)

🎯 TP1: ${sig["tp1"]:,.4f}  (+{tp1_pct:.2f}%)
   Ratio: {sig["rr_tp1"]}:1

🏆 TP2: ${sig["tp2"]:,.4f}  (+{tp2_pct:.2f}%)
   Ratio: {sig["rr_tp2"]}:1
━━━━━━━━━━━━━━━━━━━━

🧠 CONFLUENCIAS ({sig["confluences"]}):
{conf}

📦 VOLUME PROFILE:
{vp_or_default}
{hist}

💪 Fuerza: {sig["strength"]}{fg_line}{manip_line}

━━━━━━━━━━━━━━━━━━━━
🛡️ GESTIÓN DE RIESGO
⚡ Apalancamiento sugerido: x{risk["leverage"]}
📐 Tamaño de posición:      ${risk["position_size_usdt"]:,.2f} USDT
💼 Capital necesario:       ${risk["capital_necesario"]:,.2f} USDT
📊 Capital arriesgado:      {risk["riesgo_pct"]}% (${risk["max_loss_usdt"]:,.2f} USDT)
💸 Pérdida máx. si SL:      -${risk["max_loss_usdt"]:,.2f} USDT
━━━━━━━━━━━━━━━━━━━━
⚠️ Opera siempre con stop loss y gestión de riesgo."""

# ══════════════════════════════════════════════════════
#  📈  GENERADOR DE GRÁFICO (mplfinance)
# ══════════════════════════════════════════════════════
def generate_chart(df, signal):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.patches as mpatches
        import matplotlib.dates as mdates
        from matplotlib.patches import FancyArrowPatch
        import io

        sub = df.tail(60).copy()
        sub = sub.set_index("timestamp")

        price   = signal["entry_price"]
        sl      = signal["sl"]
        tp1     = signal["tp1"]
        tp2     = signal["tp2"]
        direction = signal["direction"]
        symbol  = signal["symbol"]
        tf      = signal["timeframe"]
        now     = signal["timestamp"]

        fig, (ax, ax_vol) = plt.subplots(2, 1, figsize=(12, 7),
            gridspec_kw={"height_ratios": [4, 1]}, facecolor="white")
        ax.set_facecolor("white")
        ax_vol.set_facecolor("white")

        for i, (ts, row) in enumerate(sub.iterrows()):
            o, h, l, c, v = row["open"], row["high"], row["low"], row["close"], row["volume"]
            color = "#089981" if c >= o else "#f23645"
            ax.plot([i, i], [l, h], color=color, linewidth=0.8, zorder=2)
            ax.bar(i, abs(c - o), bottom=min(o, c), color=color,
                   width=0.7, zorder=3, alpha=0.95)
            ax_vol.bar(i, v, color=color, width=0.7, alpha=0.7)

        n = len(sub)
        xs = range(n)

        if direction == "LONG":
            ax.axhspan(price, tp1, alpha=0.07, color="#089981", zorder=1)
            ax.axhspan(tp1,  tp2, alpha=0.05, color="#089981", zorder=1)
            ax.axhspan(sl,  price, alpha=0.07, color="#f23645", zorder=1)
        else:
            ax.axhspan(tp1, price, alpha=0.07, color="#089981", zorder=1)
            ax.axhspan(tp2, tp1,  alpha=0.05, color="#089981", zorder=1)
            ax.axhspan(price, sl,  alpha=0.07, color="#f23645", zorder=1)

        ax.axhline(price, color="#131722", linewidth=1.2, zorder=4, label="Entrada")
        ax.axhline(tp1,   color="#089981", linewidth=1.0, linestyle="--", zorder=4)
        ax.axhline(tp2,   color="#089981", linewidth=1.0, linestyle="--", zorder=4)
        ax.axhline(sl,    color="#f23645", linewidth=1.0, linestyle="--", zorder=4)

        ema9_vals  = sub["close"].ewm(span=9,  adjust=False).mean()
        ema21_vals = sub["close"].ewm(span=21, adjust=False).mean()
        ax.plot(list(xs), ema9_vals.values,  color="#2196f3", linewidth=1.2, label="EMA9")
        ax.plot(list(xs), ema21_vals.values, color="#ff9800", linewidth=1.0, label="EMA21")

        ymin, ymax = ax.get_ylim()
        def price_label(ax, price, text, color, fontsize=8):
            ax.annotate(text, xy=(n - 0.5, price),
                xytext=(n + 0.3, price),
                va="center", ha="left", fontsize=fontsize,
                color="white", fontfamily="monospace",
                bbox=dict(boxstyle="round,pad=0.2", facecolor=color, edgecolor="none"))

        price_label(ax, tp2,  f"TP2 {tp2:,.2f}",   "#089981")
        price_label(ax, tp1,  f"TP1 {tp1:,.2f}",   "#089981")
        price_label(ax, price,f"ENT {price:,.2f}", "#2962ff")
        price_label(ax, sl,   f"SL  {sl:,.2f}",    "#f23645")

        ax.grid(True, color="#e0e3eb", linewidth=0.5, zorder=0)
        ax_vol.grid(True, color="#e0e3eb", linewidth=0.5)

        step = max(1, n // 8)
        ticks = list(range(0, n, step))
        labels = [sub.index[i].strftime("%H:%M") for i in ticks]
        ax.set_xticks(ticks); ax.set_xticklabels([])
        ax_vol.set_xticks(ticks); ax_vol.set_xticklabels(labels, fontsize=8, color="#787b86")

        ax.tick_params(axis="y", labelsize=8, colors="#787b86")
        ax_vol.tick_params(axis="y", labelsize=7, colors="#787b86")
        ax.set_xlim(-0.5, n + 2)
        ax_vol.set_xlim(-0.5, n + 2)

        dir_txt = "🔴 SHORT" if direction == "SHORT" else "🟢 LONG"
        rr1 = signal["rr_tp1"]; rr2 = signal["rr_tp2"]
        ax.set_title(
            f"{symbol} · {tf}  |  {dir_txt}  |  Entrada: ${price:,.4f}  |  "
            f"SL: ${sl:,.4f}  |  TP1: {rr1}:1  TP2: {rr2}:1  |  {now}",
            fontsize=9, color="#131722", pad=8, loc="left"
        )

        conf_text = "  ·  ".join(
            c.replace("✅","").replace("📉","").replace("📈","").replace("🔀","")
             .replace("🧱","").replace("🛡️","").replace("🌀","").replace("🕯️","")
             .replace("🔨","").replace("📌","").replace("📊","").replace("🔥","")
             .replace("💰","").replace("🎯","").replace("⚡","").replace("🔁","")
             .replace("🌅","").replace("💫","").replace("🌆","").strip()
            for c in signal["confluence_detail"]
        )
        fig.text(0.01, 0.01, f"Confluencias ({signal['confluences']}): {conf_text}",
                 fontsize=7.5, color="#787b86", va="bottom")

        fig.tight_layout(rect=[0, 0.03, 1, 1])
        fig.subplots_adjust(hspace=0.04)

        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=130, bbox_inches="tight",
                    facecolor="white", edgecolor="none")
        plt.close(fig)
        buf.seek(0)
        return buf

    except Exception as e:
        log.error(f"Error generando gráfico: {e}")
        return None

# ══════════════════════════════════════════════════════
#  📤  TELEGRAM
# ══════════════════════════════════════════════════════
def send_telegram(message):
    token = CONFIG["telegram_token"]
    for chat in CONFIG["telegram_chats"]:
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat, "text": message}, timeout=15)
            if r.status_code == 200:
                log.info(f"✅ Telegram enviado a {chat}")
            else:
                log.warning(f"❌ Error Telegram {chat}: {r.status_code}")
        except Exception as e:
            log.error(f"Error Telegram {chat}: {e}")

def send_telegram_photo(image_buf, caption, signal):
    token = CONFIG["telegram_token"]
    for chat in CONFIG["telegram_chats"]:
        try:
            image_buf.seek(0)
            r = requests.post(
                f"https://api.telegram.org/bot{token}/sendPhoto",
                data={"chat_id": chat, "caption": caption[:1024]},
                files={"photo": ("chart.png", image_buf, "image/png")},
                timeout=30
            )
            if r.status_code == 200:
                log.info(f"✅ Grafico enviado a {chat}")
            else:
                log.warning(f"❌ Error foto {chat}: {r.status_code} {r.text}")
                send_telegram(caption)
        except Exception as e:
            log.error(f"Error foto {chat}: {e}")
            send_telegram(caption)

# ══════════════════════════════════════════════════════
#  🎛️  BOTONES INLINE Y POLLING
# ══════════════════════════════════════════════════════
def send_signal_with_buttons(image_buf, caption, signal_db_id):
    token = CONFIG["telegram_token"]
    keyboard = {
        "inline_keyboard": [[
            {"text": "🎯 Apuesta", "callback_data": f"apuesta_{signal_db_id}"},
            {"text": "✅ Acierto", "callback_data": f"acierto_{signal_db_id}"},
            {"text": "❌ Fallo",   "callback_data": f"fallo_{signal_db_id}"}
        ]]
    }
    sent_ids = []
    for chat in CONFIG["telegram_chats"]:
        try:
            image_buf.seek(0)
            r = requests.post(
                f"https://api.telegram.org/bot{token}/sendPhoto",
                data={"chat_id": chat, "caption": caption[:1024],
                      "reply_markup": json.dumps(keyboard)},
                files={"photo": ("chart.png", image_buf, "image/png")},
                timeout=30
            )
            if r.status_code == 200:
                msg_id = r.json()["result"]["message_id"]
                sent_ids.append(msg_id)
                log.info(f"✅ Señal con botones enviada a {chat} msg_id={msg_id}")
            else:
                log.warning(f"❌ Error foto {chat}: {r.status_code}")
                requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                    json={"chat_id": chat, "text": caption, "reply_markup": json.dumps(keyboard)},
                    timeout=15)
        except Exception as e:
            log.error(f"Error send_signal_with_buttons {chat}: {e}")
    return sent_ids[0] if sent_ids else None

def answer_callback(callback_query_id, text):
    token = CONFIG["telegram_token"]
    try:
        requests.post(f"https://api.telegram.org/bot{token}/answerCallbackQuery",
            json={"callback_query_id": callback_query_id, "text": text, "show_alert": False},
            timeout=10)
    except: pass

def edit_message_reply_markup(chat_id, message_id, result_text):
    token = CONFIG["telegram_token"]
    new_keyboard = {"inline_keyboard": [[
        {"text": f"✔️ {result_text}", "callback_data": "done"}
    ]]}
    try:
        requests.post(f"https://api.telegram.org/bot{token}/editMessageReplyMarkup",
            json={"chat_id": chat_id, "message_id": message_id,
                  "reply_markup": json.dumps(new_keyboard)},
            timeout=10)
    except: pass

def get_detailed_stats(sess_id):
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("SELECT timeframe, strength, result FROM signals WHERE session_id=? AND result IN ('APUESTA','TP1','TP2','SL')", (sess_id,))
    rows = c.fetchall(); conn.close()

    timeframes = ["15m", "60m", "240m", "Dm"]
    fuerzas    = ["BAJA", "MEDIA", "ALTA", "MUY ALTA"]
    result = {}

    for tf in timeframes:
        result[tf] = {}
        for fuerza in fuerzas:
            matching = [r for r in rows if r[0] == tf and fuerza in r[1]]
            ap = len(matching)
            ac = sum(1 for r in matching if r[2] in ("TP1","TP2"))
            fa = sum(1 for r in matching if r[2] == "SL")
            wr = round(ac/ap*100,1) if ap > 0 else 0
            result[tf][fuerza] = {"ap": ap, "ac": ac, "fa": fa, "wr": wr}
    return result

def send_stats_message(chat_id):
    token = CONFIG["telegram_token"]
    stats = get_session_stats()
    sess_id, sess_label = get_current_session()
    all_sess = get_all_sessions_stats()
    detailed = get_detailed_stats(sess_id)

    bars = "█" * int(stats["win_rate"] / 10) + "░" * (10 - int(stats["win_rate"] / 10))

    msg = f"📊 ESTADISTICAS — {sess_label}\n\n"
    msg += f"🎯 Apuestas:   {stats['apuestas']}\n"
    msg += f"✅ Aciertos:   {stats['aciertos']}\n"
    msg += f"❌ Fallos:     {stats['fallos']}\n"
    msg += f"⏳ Pendientes: {stats['pendientes']}\n\n"
    msg += f"📈 Win Rate: {stats['win_rate']}%\n{bars}\n\n"
    msg += "━━━━━━━━━━━━━━━━━━━━\n\n"

    tf_labels = {"15m": "⏱ 15m", "60m": "🕐 1h", "240m": "🕓 4h", "Dm": "📅 Diario"}
    fuerza_emojis = {"BAJA": "▼", "MEDIA": "◆", "ALTA": "⚡", "MUY ALTA": "🔥"}

    for tf, tf_label in tf_labels.items():
        msg += f"{tf_label}:\n"
        for fuerza, emoji in fuerza_emojis.items():
            d = detailed[tf][fuerza]
            if d["ap"] > 0:
                msg += f"  {emoji} {fuerza:<9} → Ap:{d['ap']} Ac:{d['ac']} Fa:{d['fa']} WR:{d['wr']}%\n"
            else:
                msg += f"  {emoji} {fuerza:<9} → Sin datos\n"
        msg += "\n"

    msg += "━━━━━━━━━━━━━━━━━━━━"

    if len(all_sess) > 1:
        msg += "\n\n📁 HISTORIAL:\n"
        for s in all_sess:
            ended = "activa" if not s["ended"] else s["ended"][:10]
            msg += f"  {s['label']} ({s['started'][:10]}) WR:{s['win_rate']}%\n"

    keyboard = {"inline_keyboard": [[
        {"text": "🔄 Reset nueva sesion", "callback_data": "reset_session"}
    ]]}
    try:
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg,
                  "reply_markup": json.dumps(keyboard)},
            timeout=15)
    except Exception as e:
        log.error(f"Error send_stats: {e}")

last_update_id = 0

def send_abiertas_message(chat_id):
    token = CONFIG["telegram_token"]
    conn = sqlite3.connect("signals.db")
    c = conn.cursor()
    c.execute("SELECT id, timestamp, symbol, timeframe, direction, entry_price, sl, tp1, tp2, message_id FROM signals WHERE result='APUESTA' ORDER BY id DESC LIMIT 10")
    rows = c.fetchall(); conn.close()

    if not rows:
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": "No tienes operaciones abiertas."}, timeout=10)
        return

    for row in rows:
        sid, ts, symbol, tf, direction, entry, sl, tp1, tp2, msg_id = row
        dir_emoji = "🟢 LONG" if direction == "LONG" else "🔴 SHORT"
        sl_pct = abs(entry - sl) / entry * 100
        tp1_pct = abs(tp1 - entry) / entry * 100
        text = (f"Apuesta Abierta\n"
                f"{symbol} {tf} - {dir_emoji}\n"
                f"Entrada: ${entry:,.4f}\n"
                f"SL: ${sl:,.4f} (-{sl_pct:.2f}%)\n"
                f"TP1: ${tp1:,.4f} (+{tp1_pct:.2f}%)\n"
                f"Hora: {ts}")
        keyboard = {"inline_keyboard": [[
            {"text": "✅ Acierto",        "callback_data": f"acierto_{sid}"},
            {"text": "❌ Fallo",           "callback_data": f"fallo_{sid}"},
            {"text": "❎ Quitar",          "callback_data": f"quitar_{sid}"}
        ]]}
        try:
            requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": text,
                      "reply_markup": json.dumps(keyboard)}, timeout=15)
        except Exception as e:
            log.error(f"Error send_abiertas: {e}")

def poll_telegram():
    global last_update_id
    token = CONFIG["telegram_token"]
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{token}/getUpdates",
            params={"offset": last_update_id + 1, "timeout": 2, "limit": 10},
            timeout=8
        )
        if r.status_code != 200: return
        updates = r.json().get("result", [])
        for upd in updates:
            last_update_id = upd["update_id"]

            if "callback_query" in upd:
                cq      = upd["callback_query"]
                cq_id   = cq["id"]
                data    = cq["data"]
                chat_id = cq["message"]["chat"]["id"]
                msg_id  = cq["message"]["message_id"]

                if data.startswith("apuesta_"):
                    sig_id = data.split("_")[1]
                    update_signal_result(msg_id, "APUESTA")
                    answer_callback(cq_id, "🎯 Apuesta registrada — usa /abiertas para cerrarla")
                    new_kb = {"inline_keyboard": [[
                        {"text": "✅ Acierto",        "callback_data": f"acierto_{sig_id}"},
                        {"text": "❌ Fallo",           "callback_data": f"fallo_{sig_id}"},
                        {"text": "❎ Quitar apuesta", "callback_data": f"quitar_{sig_id}"}
                    ]]}
                    try:
                        requests.post(f"https://api.telegram.org/bot{token}/editMessageReplyMarkup",
                            json={"chat_id": chat_id, "message_id": msg_id,
                                  "reply_markup": json.dumps(new_kb)}, timeout=10)
                    except: pass
                    # Añadir al monitor de posiciones
                    conn2 = sqlite3.connect("signals.db")
                    c2 = conn2.cursor()
                    c2.execute("SELECT symbol, direction, entry_price, sl, tp1, tp2 FROM signals WHERE id=? OR message_id=?",
                               (sig_id, str(msg_id)))
                    row = c2.fetchone(); conn2.close()
                    if row:
                        symbol, direction, entry_price, sl, tp1, tp2 = row
                        with active_positions_lock:
                            active_positions[sig_id] = {
                                "symbol":      symbol,
                                "direction":   direction,
                                "entry_price": entry_price,
                                "sl":          sl,
                                "tp1":         tp1,
                                "tp2":         tp2,
                                "message_id":  msg_id,
                                "hit_50":      False,
                                "hit_tp1":     False,
                            }
                        log.info(f"🎯 Apuesta registrada y monitorizando {symbol}")
                    log.info(f"🎯 Apuesta registrada msg={msg_id}")

                elif data.startswith("acierto_"):
                    sig_id = data.split("_")[1]
                    conn2 = sqlite3.connect("signals.db")
                    conn2.cursor().execute("UPDATE signals SET result='TP1' WHERE id=? OR message_id=?", (sig_id, str(msg_id)))
                    conn2.commit(); conn2.close()
                    answer_callback(cq_id, "✅ Acierto registrado!")
                    try:
                        done_kb = {"inline_keyboard": [[{"text": "✅ Acierto confirmado", "callback_data": "done"}]]}
                        requests.post(f"https://api.telegram.org/bot{token}/editMessageReplyMarkup",
                            json={"chat_id": chat_id, "message_id": msg_id, "reply_markup": json.dumps(done_kb)}, timeout=10)
                    except: pass
                    log.info(f"✅ Acierto registrado sig={sig_id}")

                elif data.startswith("fallo_"):
                    sig_id = data.split("_")[1]
                    conn2 = sqlite3.connect("signals.db")
                    conn2.cursor().execute("UPDATE signals SET result='SL' WHERE id=? OR message_id=?", (sig_id, str(msg_id)))
                    conn2.commit(); conn2.close()
                    answer_callback(cq_id, "❌ Fallo registrado")
                    try:
                        done_kb = {"inline_keyboard": [[{"text": "❌ Fallo confirmado", "callback_data": "done"}]]}
                        requests.post(f"https://api.telegram.org/bot{token}/editMessageReplyMarkup",
                            json={"chat_id": chat_id, "message_id": msg_id, "reply_markup": json.dumps(done_kb)}, timeout=10)
                    except: pass
                    log.info(f"❌ Fallo registrado sig={sig_id}")

                elif data.startswith("quitar_"):
                    sig_id = data.split("_")[1]
                    conn2 = sqlite3.connect("signals.db")
                    conn2.cursor().execute("UPDATE signals SET result='PENDING' WHERE id=? OR message_id=?", (sig_id, str(msg_id)))
                    conn2.commit(); conn2.close()
                    answer_callback(cq_id, "❎ Apuesta eliminada")
                    # Quitar del monitor
                    with active_positions_lock:
                        active_positions.pop(sig_id, None)
                    orig_kb = {"inline_keyboard": [[
                        {"text": "🎯 Apuesta",  "callback_data": f"apuesta_{sig_id}"},
                        {"text": "✅ Acierto",  "callback_data": f"acierto_{sig_id}"},
                        {"text": "❌ Fallo",    "callback_data": f"fallo_{sig_id}"}
                    ]]}
                    try:
                        requests.post(f"https://api.telegram.org/bot{token}/editMessageReplyMarkup",
                            json={"chat_id": chat_id, "message_id": msg_id,
                                  "reply_markup": json.dumps(orig_kb)}, timeout=10)
                    except: pass
                    log.info(f"❎ Apuesta quitada sig={sig_id}")

                elif data == "reset_session":
                    label = reset_session()
                    answer_callback(cq_id, f"🔄 Nueva sesión iniciada: {label}")
                    requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                        json={"chat_id": chat_id,
                              "text": f"🔄 Sesion reseteada. Nueva sesion: {label}\nEstadisticas anteriores guardadas."},
                        timeout=10)

            elif "message" in upd:
                msg  = upd["message"]
                text = msg.get("text", "")
                chat_id = msg["chat"]["id"]
                if text.startswith("/stats"):
                    send_stats_message(chat_id)
                elif text.startswith("/abiertas"):
                    send_abiertas_message(chat_id)
                elif text.startswith("/reset"):
                    label = reset_session()
                    requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                        json={"chat_id": chat_id,
                              "text": f"🔄 Nueva sesion: {label}\nEstadisticas guardadas."},
                        timeout=10)
                elif text.startswith("/posiciones"):
                    send_posiciones_message(chat_id)
                elif text.startswith("/parar"):
                    global bot_running
                    bot_running = False
                    requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                        json={"chat_id": chat_id,
                              "text": "⏸️ Bot pausado — no se enviarán nuevas señales.\nUsa /arrancar para reanudar."},
                        timeout=10)
                    log.info("⏸️ Bot pausado por Telegram")
                elif text.startswith("/arrancar"):
                    bot_running = True
                    requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                        json={"chat_id": chat_id,
                              "text": "▶️ Bot reanudado — analizando mercados."},
                        timeout=10)
                    log.info("▶️ Bot reanudado por Telegram")
                elif text.startswith("/ayuda") or text == "/help":
                    requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                        json={"chat_id": chat_id, "text": (
                            "📖 COMANDOS DISPONIBLES\n\n"
                            "📊 /stats — estadísticas de la sesión\n"
                            "📍 /posiciones — posiciones activas con P&L\n"
                            "🔓 /abiertas — operaciones abiertas\n"
                            "💼 /capital [cantidad] [riesgo%] — cambiar capital\n"
                            "     Ejemplo: /capital 2000 1.5\n"
                            "💰 /precio [par] — precio actual\n"
                            "     Ejemplo: /precio BTCUSDT\n"
                            "⏸️ /parar — pausar el bot\n"
                            "▶️ /arrancar — reanudar el bot\n"
                            "🔄 /reset — nueva sesión de estadísticas\n\n"
                            "🎯 Botones en señales:\n"
                            "  🎯 Apuesta → activa monitor automático\n"
                            "  ✅ Acierto / ❌ Fallo → registrar manual\n"
                            "  ❎ Quitar → cancelar apuesta\n\n"
                            "📅 Resumen automático cada día a las 23:00"
                        )}, timeout=10)
                elif text.startswith("/precio"):
                    parts = text.split()
                    symbol = parts[1].upper() if len(parts) > 1 else "BTCUSDT"
                    price = get_current_price(symbol)
                    if price:
                        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                            json={"chat_id": chat_id,
                                  "text": f"💰 {symbol}\n📍 Precio: ${price:,.4f}\n🕐 {spain_now()} (España)"},
                            timeout=10)
                    else:
                        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                            json={"chat_id": chat_id,
                                  "text": f"❌ No se pudo obtener el precio de {symbol}.\nVerifica que el par existe en Bybit."},
                            timeout=10)
                    # Formato: /capital 2000 o /capital 2000 1.5
                    parts = text.split()
                    try:
                        nuevo_capital = float(parts[1])
                        CONFIG["capital_total"] = nuevo_capital
                        nuevo_riesgo = float(parts[2]) if len(parts) > 2 else CONFIG["riesgo_pct"]
                        CONFIG["riesgo_pct"] = nuevo_riesgo
                        max_loss = round(nuevo_capital * nuevo_riesgo / 100, 2)
                        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                            json={"chat_id": chat_id,
                                  "text": (f"✅ Capital actualizado\n"
                                           f"💼 Capital: ${nuevo_capital:,.2f} USDT\n"
                                           f"📊 Riesgo: {nuevo_riesgo}%\n"
                                           f"💸 Pérdida máx. por op: ${max_loss:,.2f} USDT")},
                            timeout=10)
                        log.info(f"💼 Capital actualizado a ${nuevo_capital} riesgo {nuevo_riesgo}%")
                    except (IndexError, ValueError):
                        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                            json={"chat_id": chat_id,
                                  "text": (f"ℹ️ Uso: /capital <cantidad> [riesgo%]\n"
                                           f"Ejemplo: /capital 2000\n"
                                           f"Ejemplo: /capital 2000 1.5\n\n"
                                           f"Capital actual: ${CONFIG['capital_total']:,.2f}\n"
                                           f"Riesgo actual: {CONFIG['riesgo_pct']}%")},
                            timeout=10)

    except Exception as e:
        log.error(f"Error polling: {e}")

# ══════════════════════════════════════════════════════
#  📊  DERIBIT — VOLUMEN DE OPCIONES
# ══════════════════════════════════════════════════════
def fetch_deribit_options(symbol):
    """
    Consulta Deribit para ver el ratio put/call de opciones.
    Solo para BTC y ETH.
    - Ratio > 1.2 → más puts que calls → presión bajista
    - Ratio < 0.8 → más calls que puts → presión alcista
    """
    currency_map = {"BTCUSDT": "BTC", "ETHUSDT": "ETH"}
    currency = currency_map.get(symbol)
    if not currency:
        return None
    try:
        r = requests.get(
            f"https://deribit.com/api/v2/public/get_book_summary_by_currency",
            params={"currency": currency, "kind": "option"},
            timeout=8
        )
        data = r.json().get("result", [])
        calls = sum(float(x.get("volume", 0)) for x in data if "-C" in x.get("instrument_name",""))
        puts  = sum(float(x.get("volume", 0)) for x in data if "-P" in x.get("instrument_name",""))
        if calls + puts == 0:
            return None
        ratio = round(puts / calls, 2) if calls > 0 else 99
        log.info(f"📊 Deribit {currency} — puts/calls ratio: {ratio}")
        return {"ratio": ratio, "calls": round(calls, 0), "puts": round(puts, 0)}
    except Exception as e:
        log.warning(f"⚠️ Deribit no disponible: {e}")
        return None

# ══════════════════════════════════════════════════════
#  😱  FEAR & GREED INDEX
# ══════════════════════════════════════════════════════
def fetch_fear_greed():
    """Obtiene el índice Fear & Greed de Alternative.me"""
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=8)
        data = r.json()["data"][0]
        value = int(data["value"])
        label = data["value_classification"]
        return {"value": value, "label": label}
    except Exception as e:
        log.warning(f"⚠️ Fear & Greed no disponible: {e}")
        return None

# ══════════════════════════════════════════════════════
#  📈  MONITOR DE PRECIOS — TP/SL en tiempo real
# ══════════════════════════════════════════════════════
active_positions = {}  # {signal_id: signal_data}
active_positions_lock = threading.Lock()

def get_current_price(symbol):
    """Obtiene el precio actual de un símbolo"""
    try:
        r = requests.get("https://api.bybit.com/v5/market/tickers",
            params={"category": "linear", "symbol": symbol}, timeout=5)
        items = r.json()["result"]["list"]
        if items:
            return float(items[0]["lastPrice"])
    except: pass
    return None

def spain_now():
    from datetime import timedelta
    return (datetime.now(timezone.utc) + timedelta(hours=CONFIG.get("utc_offset", 2))).strftime("%H:%M")

def auto_register_result(sig_id, msg_id, result, button_text):
    """Registra resultado en BD y actualiza botón en Telegram automáticamente"""
    with db_lock:
        conn2 = sqlite3.connect("signals.db")
        conn2.cursor().execute(
            "UPDATE signals SET result=? WHERE id=? OR message_id=?",
            (result, sig_id, str(msg_id or "")))
        conn2.commit(); conn2.close()
    if msg_id:
        token = CONFIG["telegram_token"]
        done_kb = {"inline_keyboard": [[{"text": button_text, "callback_data": "done"}]]}
        for chat in CONFIG["telegram_chats"]:
            try:
                requests.post(
                    f"https://api.telegram.org/bot{token}/editMessageReplyMarkup",
                    json={"chat_id": chat, "message_id": int(msg_id),
                          "reply_markup": json.dumps(done_kb)}, timeout=10)
            except: pass

def monitor_positions():
    """
    Hilo que monitoriza posiciones con apuesta activa cada 2 minutos.
    Avisa y registra automáticamente cuando el precio toca 50%TP1, TP1, TP2 o SL.
    """
    while True:
        time.sleep(120)
        with active_positions_lock:
            positions = dict(active_positions)

        for sig_id, sig in positions.items():
            try:
                symbol    = sig["symbol"]
                direction = sig["direction"]
                entry     = sig["entry_price"]
                sl        = sig["sl"]
                tp1       = sig["tp1"]
                tp2       = sig["tp2"]
                msg_id    = sig.get("message_id")
                price     = get_current_price(symbol)
                if not price:
                    continue

                hit_50  = sig.get("hit_50",  False)
                hit_tp1 = sig.get("hit_tp1", False)

                if direction == "LONG":
                    half_tp1 = entry + (tp1 - entry) * 0.5

                    # 50% hacia TP1 — trailing stop
                    if not hit_50 and price >= half_tp1:
                        new_sl = round(entry * 1.001, 4)
                        send_telegram(
                            f"⚡ TRAILING STOP — {symbol}\n"
                            f"📍 Precio: ${price:,.4f}\n"
                            f"✅ 50% camino a TP1 alcanzado\n"
                            f"🔒 Mueve SL a breakeven: ${new_sl:,.4f}\n"
                            f"🕐 {spain_now()} (España)"
                        )
                        with active_positions_lock:
                            active_positions[sig_id]["hit_50"] = True

                    # TP2 — registrar primero (prioridad)
                    if price >= tp2:
                        auto_register_result(sig_id, msg_id, "TP2", "🏆 TP2 automático")
                        send_telegram(
                            f"🏆 TP2 ALCANZADO — {symbol} 🎉\n"
                            f"📍 Precio: ${price:,.4f}\n"
                            f"✅ TP2: ${tp2:,.4f} tocado\n"
                            f"💰 Operación completada\n"
                            f"📊 Registrado automáticamente\n"
                            f"🕐 {spain_now()} (España)"
                        )
                        with active_positions_lock:
                            active_positions.pop(sig_id, None)

                    # TP1
                    elif not hit_tp1 and price >= tp1:
                        new_sl = round(entry * 1.002, 4)
                        auto_register_result(sig_id, msg_id, "TP1", "✅ TP1 automático")
                        send_telegram(
                            f"🎯 TP1 ALCANZADO — {symbol} ✅\n"
                            f"📍 Precio: ${price:,.4f}\n"
                            f"✅ TP1: ${tp1:,.4f} tocado\n"
                            f"🔒 Mueve SL a: ${new_sl:,.4f} (entrada+)\n"
                            f"🏆 Objetivo TP2: ${tp2:,.4f}\n"
                            f"📊 Registrado como ACIERTO automáticamente\n"
                            f"🕐 {spain_now()} (España)"
                        )
                        with active_positions_lock:
                            active_positions[sig_id]["hit_tp1"] = True

                    # SL
                    elif price <= sl:
                        auto_register_result(sig_id, msg_id, "SL", "❌ SL automático")
                        send_telegram(
                            f"🛑 STOP LOSS — {symbol}\n"
                            f"📍 Precio: ${price:,.4f}\n"
                            f"❌ SL: ${sl:,.4f} tocado\n"
                            f"📊 Registrado como FALLO automáticamente\n"
                            f"🕐 {spain_now()} (España)"
                        )
                        with active_positions_lock:
                            active_positions.pop(sig_id, None)

                else:  # SHORT
                    half_tp1 = entry - (entry - tp1) * 0.5

                    if not hit_50 and price <= half_tp1:
                        new_sl = round(entry * 0.999, 4)
                        send_telegram(
                            f"⚡ TRAILING STOP — {symbol}\n"
                            f"📍 Precio: ${price:,.4f}\n"
                            f"✅ 50% camino a TP1 alcanzado\n"
                            f"🔒 Mueve SL a breakeven: ${new_sl:,.4f}\n"
                            f"🕐 {spain_now()} (España)"
                        )
                        with active_positions_lock:
                            active_positions[sig_id]["hit_50"] = True

                    if price <= tp2:
                        auto_register_result(sig_id, msg_id, "TP2", "🏆 TP2 automático")
                        send_telegram(
                            f"🏆 TP2 ALCANZADO — {symbol} 🎉\n"
                            f"📍 Precio: ${price:,.4f}\n"
                            f"✅ TP2: ${tp2:,.4f} tocado\n"
                            f"💰 Operación completada\n"
                            f"📊 Registrado automáticamente\n"
                            f"🕐 {spain_now()} (España)"
                        )
                        with active_positions_lock:
                            active_positions.pop(sig_id, None)

                    elif not hit_tp1 and price <= tp1:
                        new_sl = round(entry * 0.998, 4)
                        auto_register_result(sig_id, msg_id, "TP1", "✅ TP1 automático")
                        send_telegram(
                            f"🎯 TP1 ALCANZADO — {symbol} ✅\n"
                            f"📍 Precio: ${price:,.4f}\n"
                            f"✅ TP1: ${tp1:,.4f} tocado\n"
                            f"🔒 Mueve SL a: ${new_sl:,.4f} (entrada-)\n"
                            f"🏆 Objetivo TP2: ${tp2:,.4f}\n"
                            f"📊 Registrado como ACIERTO automáticamente\n"
                            f"🕐 {spain_now()} (España)"
                        )
                        with active_positions_lock:
                            active_positions[sig_id]["hit_tp1"] = True

                    elif price >= sl:
                        auto_register_result(sig_id, msg_id, "SL", "❌ SL automático")
                        send_telegram(
                            f"🛑 STOP LOSS — {symbol}\n"
                            f"📍 Precio: ${price:,.4f}\n"
                            f"❌ SL: ${sl:,.4f} tocado\n"
                            f"📊 Registrado como FALLO automáticamente\n"
                            f"🕐 {spain_now()} (España)"
                        )
                        with active_positions_lock:
                            active_positions.pop(sig_id, None)

            except Exception as e:
                log.error(f"Error monitor {sig_id}: {e}")

# ══════════════════════════════════════════════════════
#  📅  RESUMEN DIARIO
# ══════════════════════════════════════════════════════
def send_daily_summary():
    """Envía resumen diario a las 23:00 hora española"""
    while True:
        from datetime import timedelta
        now_spain = datetime.now(timezone.utc) + timedelta(hours=CONFIG.get("utc_offset", 2))
        # Calcular segundos hasta las 23:00
        target = now_spain.replace(hour=23, minute=0, second=0, microsecond=0)
        if now_spain >= target:
            target = target + timedelta(days=1)
        secs = (target - now_spain).total_seconds()
        time.sleep(secs)

        # Generar resumen
        try:
            conn = sqlite3.connect("signals.db")
            c = conn.cursor()
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            c.execute("SELECT * FROM signals WHERE timestamp LIKE ?", (f"{today}%",))
            rows = c.fetchall(); conn.close()

            total     = len(rows)
            apuestas  = sum(1 for r in rows if r[14] in ("APUESTA","TP1","TP2","SL"))
            aciertos  = sum(1 for r in rows if r[14] in ("TP1","TP2"))
            fallos    = sum(1 for r in rows if r[14] == "SL")
            pendientes= sum(1 for r in rows if r[14] == "PENDING")
            wr = round(aciertos / apuestas * 100, 1) if apuestas > 0 else 0

            # Mejores señales del día (mayor R/R)
            best = sorted(rows, key=lambda r: r[11] or 0, reverse=True)[:3]
            best_txt = ""
            for b in best:
                best_txt += f"  • {b[2]} {b[3]} {b[4]} — R/R {b[11]}\n"

            bars = "█" * int(wr / 10) + "░" * (10 - int(wr / 10))

            msg = (
                f"📅 RESUMEN DEL DÍA — {today}\n\n"
                f"📊 Señales totales: {total}\n"
                f"🎯 Apuestas:  {apuestas}\n"
                f"✅ Aciertos:  {aciertos}\n"
                f"❌ Fallos:    {fallos}\n"
                f"⏳ Pendientes:{pendientes}\n\n"
                f"📈 Win Rate: {wr}%\n{bars}\n\n"
            )
            if best_txt:
                msg += f"🏆 Mejores señales:\n{best_txt}\n"
            msg += "⚠️ Opera siempre con stop loss."

            send_telegram(msg)
            log.info("📅 Resumen diario enviado")
        except Exception as e:
            log.error(f"Error resumen diario: {e}")

# ══════════════════════════════════════════════════════
#  📍  COMANDO /posiciones
# ══════════════════════════════════════════════════════
def send_posiciones_message(chat_id):
    token = CONFIG["telegram_token"]
    with active_positions_lock:
        positions = dict(active_positions)

    if not positions:
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id,
                  "text": "📭 No tienes posiciones monitorizadas ahora mismo."},
            timeout=10)
        return

    msg = f"📍 POSICIONES ACTIVAS ({len(positions)})\n\n"
    for sig_id, sig in positions.items():
        price = get_current_price(sig["symbol"])
        entry = sig["entry_price"]
        sl    = sig["sl"]
        tp1   = sig["tp1"]
        tp2   = sig["tp2"]
        direction = sig["direction"]

        if price:
            if direction == "LONG":
                pnl_pct = (price - entry) / entry * 100
            else:
                pnl_pct = (entry - price) / entry * 100
            pnl_emoji = "🟢" if pnl_pct >= 0 else "🔴"
            price_line = f"📍 Precio actual: ${price:,.4f}\n{pnl_emoji} P&L estimado: {pnl_pct:+.2f}%"
        else:
            price_line = "📍 Precio: no disponible"

        dir_emoji = "📈" if direction == "LONG" else "📉"
        trailing = "✅ Trailing activo" if sig.get("hit_50") else "⏳ Esperando 50% TP1"

        msg += (f"{'─'*20}\n"
                f"{dir_emoji} {sig['symbol']} — {direction}\n"
                f"💰 Entrada: ${entry:,.4f}\n"
                f"{price_line}\n"
                f"🛑 SL: ${sl:,.4f}\n"
                f"🎯 TP1: ${tp1:,.4f}\n"
                f"🏆 TP2: ${tp2:,.4f}\n"
                f"⚡ {trailing}\n\n")

    msg += f"🕐 {spain_now()} (España)"
    try:
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg}, timeout=15)
    except Exception as e:
        log.error(f"Error send_posiciones: {e}")

# ══════════════════════════════════════════════════════
#  🌐  EXPORTAR AL DASHBOARD
# ══════════════════════════════════════════════════════
def export_to_web(new_signal=None):
    global recent_signals_cache
    try:
        if new_signal:
            risk = calculate_risk(new_signal)
            recent_signals_cache.insert(0, {
                "symbol":            new_signal["symbol"],
                "timeframe":         new_signal["timeframe"],
                "direction":         new_signal["direction"],
                "entry_price":       new_signal["entry_price"],
                "sl":                new_signal["sl"],
                "tp1":               new_signal["tp1"],
                "tp2":               new_signal["tp2"],
                "rr_tp1":            new_signal["rr_tp1"],
                "rr_tp2":            new_signal["rr_tp2"],
                "confluences":       new_signal["confluences"],
                "confluence_detail": new_signal["confluence_detail"],
                "strength":          new_signal["strength"],
                "timestamp":         new_signal["timestamp"],
                "risk":              risk,
            })
            recent_signals_cache = recent_signals_cache[:10]

        with active_positions_lock:
            pos_list = [
                {"symbol": v["symbol"], "direction": v["direction"],
                 "entry_price": v["entry_price"], "sl": v["sl"],
                 "tp1": v["tp1"], "tp2": v["tp2"]}
                for v in active_positions.values()
            ]
        from datetime import timedelta
        spain_now_dt = datetime.now(timezone.utc) + timedelta(hours=CONFIG.get("utc_offset", 2))
        data = {
            "last_update":     spain_now_dt.strftime("%Y-%m-%d %H:%M:%S (España)"),
            "bot_status":      "ACTIVO" if bot_running else "PAUSADO",
            "signals_today":   len(recent_signals_cache),
            "pairs_monitored": CONFIG["symbols"],
            "n_workers":       len(CONFIG["symbols"]) * len(CONFIG["timeframes"]),
            "recent_signals":  recent_signals_cache,
            "active_positions": pos_list,
            "risk_config": {
                "capital_total": CONFIG.get("capital_total", 1000),
                "riesgo_pct":    CONFIG.get("riesgo_pct", 1.0),
            },
        }
        with open("signals_live.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        subprocess.run(["git", "add", "signals_live.json"], capture_output=True)
        subprocess.run(["git", "commit", "-m", "Update live signals"], capture_output=True)
        result = subprocess.run(["git", "push"], capture_output=True)
        log.info("📊 Dashboard actualizado" if result.returncode == 0 else "⚠️ Git push falló")
    except Exception as e:
        log.error(f"Error export_to_web: {e}")

# ══════════════════════════════════════════════════════
#  ⏱️  COOLDOWN
# ══════════════════════════════════════════════════════
def should_send(symbol, timeframe, strength=""):
    if "MUY ALTA" in strength:
        log.info(f"🔥 MUY ALTA — ignorando cooldown para {symbol} {timeframe}")
        return True
    key  = f"{symbol}_{timeframe}"
    last = last_signal_time.get(key)
    if last is None: return True
    return (datetime.now(timezone.utc) - last).total_seconds() / 60 >= CONFIG["cooldown_minutes"]

# ══════════════════════════════════════════════════════
#  🚨  ALERTAS DE MERCADO EXTREMO
# ══════════════════════════════════════════════════════
def monitor_market_extremes():
    """
    Monitoriza BTC cada 5 minutos.
    Si sube o baja más de un 5% en 1 hora → alerta inmediata.
    """
    price_history = []  # [(timestamp, price)]
    while True:
        time.sleep(300)  # Cada 5 minutos
        try:
            price = get_current_price("BTCUSDT")
            if not price:
                continue

            now = datetime.now(timezone.utc)
            price_history.append((now, price))

            # Mantener solo la última hora
            price_history = [(t, p) for t, p in price_history
                             if (now - t).total_seconds() <= 3600]

            if len(price_history) < 3:
                continue

            oldest_price = price_history[0][1]
            change_pct = (price - oldest_price) / oldest_price * 100

            if abs(change_pct) >= 5.0:
                direction = "📈 SUBIDA" if change_pct > 0 else "📉 BAJADA"
                emoji = "🚀" if change_pct > 0 else "💥"
                send_telegram(
                    f"{emoji} MOVIMIENTO EXTREMO — BTCUSDT\n\n"
                    f"{direction} del {change_pct:+.2f}% en 1 hora\n"
                    f"📍 Precio actual: ${price:,.2f}\n"
                    f"📍 Hace 1h: ${oldest_price:,.2f}\n\n"
                    f"⚠️ Mercado volátil — opera con precaución\n"
                    f"🕐 {spain_now()} (España)"
                )
                log.info(f"🚨 Alerta extremo BTC {change_pct:+.2f}%")
                # Limpiar historial para no repetir la alerta
                price_history = [(now, price)]

        except Exception as e:
            log.error(f"Error monitor_market_extremes: {e}")

# ══════════════════════════════════════════════════════
#  ⏰  ESPERAR CIERRE DE VELA
# ══════════════════════════════════════════════════════
def seconds_to_candle_close(timeframe):
    now = datetime.now(timezone.utc)
    if timeframe == "15":
        minutes_in_tf = 15
    elif timeframe == "60":
        minutes_in_tf = 60
    elif timeframe == "240":
        minutes_in_tf = 240
    elif timeframe == "D":
        from datetime import timedelta
        next_close = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        return (next_close - now).total_seconds()
    else:
        return 0

    total_seconds = minutes_in_tf * 60
    elapsed = (now.minute % minutes_in_tf) * 60 + now.second
    remaining = total_seconds - elapsed
    return remaining

def is_near_candle_close(timeframe, seconds_before=30):
    remaining = seconds_to_candle_close(timeframe)
    return remaining <= seconds_before

def wait_for_candle_close(timeframe):
    remaining = seconds_to_candle_close(timeframe)
    if remaining > 60:
        return False
    if remaining > 0:
        log.info(f"  ⏰ Esperando cierre de vela {timeframe} — {remaining:.0f}s")
        time.sleep(remaining + 3)
    return True

# ══════════════════════════════════════════════════════
#  ⚡  ANÁLISIS EN PARALELO
# ══════════════════════════════════════════════════════
def process_pair(symbol, tf):
    """Analiza un par/timeframe — se ejecuta en un hilo separado"""
    global bot_running
    if not bot_running:
        log.info(f"⏸️ Bot pausado — saltando {symbol} {tf}")
        return
    try:
        remaining = seconds_to_candle_close(tf)

        if remaining > 120:
            log.info(f"⏳ {symbol} {tf} — cierre en {remaining/60:.1f}min, saltando")
            return

        log.info(f"🔍 [{symbol} {tf}] Analizando — cierre en {remaining:.0f}s")

        # Esperar cierre de vela si falta poco
        if remaining > 0:
            log.info(f"  ⏰ [{symbol} {tf}] Esperando cierre — {remaining:.0f}s")
            time.sleep(remaining + 3)

        df = fetch_candles(symbol, tf, limit=300)
        if df is None or len(df) < 100:
            return

        signal = analyze(df, symbol, tf)

        if signal:
            log.info(f"🚨 SEÑAL {signal['direction']} {symbol} {tf}m | "
                     f"TP1:{signal['rr_tp1']} TP2:{signal['rr_tp2']} | "
                     f"{signal['confluences']} confluencias")
            if should_send(symbol, tf, signal["strength"]):
                msg          = build_message(signal)
                signal_db_id = save_signal(signal)
                chart        = generate_chart(df, signal)
                if chart:
                    sent_msg_id = send_signal_with_buttons(chart, msg, signal_db_id)
                    if sent_msg_id:
                        save_message_id(signal_db_id, sent_msg_id)
                else:
                    send_telegram(msg)
                export_to_web(signal)
                last_signal_time[f"{symbol}_{tf}"] = datetime.now(timezone.utc)
                register_signal_direction(symbol, signal["direction"])
            else:
                log.info(f"⏸️ Cooldown activo {symbol} {tf}m")
        else:
            log.info(f"⏳ Sin señal — {symbol} {tf}m")

    except Exception as e:
        log.error(f"❌ Error {symbol} {tf}m: {e}")


# ══════════════════════════════════════════════════════
#  🚀  LOOP PRINCIPAL
# ══════════════════════════════════════════════════════
def next_candle_sleep():
    """
    Calcula cuántos segundos esperar hasta que el próximo
    cierre de vela esté a menos de 2 minutos.
    Así el bot no hace ciclos innecesarios.
    """
    timeframes = [tf for tf in CONFIG["timeframes"] if tf != "D"]
    min_remaining = min(seconds_to_candle_close(tf) for tf in timeframes)

    # Si ya hay una vela cerrando en menos de 2 min → no dormir
    if min_remaining <= 120:
        return 0

    # Dormir hasta que falten 90 segundos para el próximo cierre
    sleep_time = min_remaining - 90
    return max(int(sleep_time), 10)  # Mínimo 10s por seguridad


def run():
    log.info("🚀 MARCO TRADING BOT v4 INICIADO — ANÁLISIS PARALELO")
    log.info(f"📊 Pares: {CONFIG['symbols']} | Timeframes: {CONFIG['timeframes']}")
    log.info(f"🎯 Min confluencias: {CONFIG['min_confluences']} | TP1: {CONFIG['min_rr_tp1']} | TP2: {CONFIG['min_rr_tp2']}")
    log.info(f"🛡️ Capital: ${CONFIG['capital_total']} USDT | Riesgo: {CONFIG['riesgo_pct']}% por operación")

    n_workers = len(CONFIG["symbols"]) * len(CONFIG["timeframes"])
    log.info(f"⚡ Hilos en paralelo: {n_workers}")

    init_db()

    # Arrancar hilos de fondo
    threading.Thread(target=monitor_positions,      daemon=True, name="monitor").start()
    threading.Thread(target=send_daily_summary,     daemon=True, name="daily").start()
    threading.Thread(target=monitor_market_extremes, daemon=True, name="extremes").start()
    log.info("🔍 Monitor de posiciones activo (cada 2min)")
    log.info("📅 Resumen diario programado a las 23:00 (España)")
    log.info("🚨 Monitor de mercado extremo activo (cada 5min)")
    send_telegram(
        f"🤖 Marco Trading Bot v4 iniciado\n"
        f"⚡ Análisis en paralelo activo ({n_workers} hilos)\n"
        f"✅ Botones de estadísticas activos\n"
        f"📖 /ayuda — ver todos los comandos\n"
        f"🛡️ Gestión de riesgo activa — {CONFIG['riesgo_pct']}% por operación"
    )
    export_to_web()

    pairs = [(s, tf) for s in CONFIG["symbols"] for tf in CONFIG["timeframes"]]

    while True:
        log.info(f"🔄 Iniciando ciclo — analizando {len(pairs)} combinaciones en paralelo")

        with concurrent.futures.ThreadPoolExecutor(max_workers=n_workers) as executor:
            futures = {executor.submit(process_pair, s, tf): (s, tf) for s, tf in pairs}
            for future in concurrent.futures.as_completed(futures):
                s, tf = futures[future]
                if future.exception():
                    log.error(f"❌ Hilo {s} {tf}: {future.exception()}")

        # Calcular tiempo hasta el próximo cierre de vela
        sleep_secs = next_candle_sleep()

        if sleep_secs > 0:
            log.info(f"😴 Próximo cierre en ~{(sleep_secs+90)//60}min — durmiendo {sleep_secs}s")
            # Dormir en bloques de 5s para seguir haciendo polling de Telegram
            slept = 0
            while slept < sleep_secs:
                poll_telegram()
                chunk = min(5, sleep_secs - slept)
                time.sleep(chunk)
                slept += chunk
        else:
            log.info(f"⚡ Cierre inminente — relanzando ciclo inmediatamente")
            # Poll rápido antes del siguiente ciclo
            poll_telegram()

if __name__ == "__main__":
    run()
