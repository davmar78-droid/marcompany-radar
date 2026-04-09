import requests
import pandas as pd
import pandas_ta as ta
import numpy as np
import json, time, logging, sqlite3, subprocess
from datetime import datetime, timezone

# ══════════════════════════════════════════════════════
#  ⚙️  CONFIGURACIÓN
# ══════════════════════════════════════════════════════
CONFIG = {
    "telegram_token":   "8135742976:AAHK6NPEYrb90IGGj764RqCoqLXVIPywgBU",
    "telegram_chats":   ["-1003893933581", "772021739"],
    "symbols":          ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"],
    "timeframes":       ["15", "60"],
    "min_rr_tp1":       2.0,
    "min_rr_tp2":       4.0,
    "min_confluences":  4,
    "check_interval":   60,
    "cooldown_minutes": 45,
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

# ══════════════════════════════════════════════════════
#  🗄️  BASE DE DATOS
# ══════════════════════════════════════════════════════
def init_db():
    conn = sqlite3.connect("signals.db")
    conn.cursor().execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT, symbol TEXT, timeframe TEXT, direction TEXT,
            entry_price REAL, sl REAL, tp1 REAL, tp2 REAL,
            rr_tp1 REAL, rr_tp2 REAL, confluences INTEGER,
            confluence_detail TEXT, strength TEXT, result TEXT DEFAULT 'PENDING'
        )
    """)
    conn.commit(); conn.close()
    log.info("✅ Base de datos lista.")

def save_signal(sig):
    conn = sqlite3.connect("signals.db")
    conn.cursor().execute("""
        INSERT INTO signals (timestamp,symbol,timeframe,direction,entry_price,
        sl,tp1,tp2,rr_tp1,rr_tp2,confluences,confluence_detail,strength)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (sig["timestamp"], sig["symbol"], sig["timeframe"], sig["direction"],
          sig["entry_price"], sig["sl"], sig["tp1"], sig["tp2"],
          sig["rr_tp1"], sig["rr_tp2"], sig["confluences"],
          json.dumps(sig["confluence_detail"]), sig["strength"]))
    conn.commit(); conn.close()

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
    df["vol_sma20"] = df["volume"].rolling(20).mean()
    return df

# ══════════════════════════════════════════════════════
#  🕯️  PATRONES DE VELA
# ══════════════════════════════════════════════════════
def detect_candles(df):
    p = {"bull_engulf": False, "bear_engulf": False, "hammer": False,
         "shooting_star": False, "pin_bull": False, "pin_bear": False,
         "morning_star": False, "evening_star": False}
    if len(df) < 4: return p
    c     = df.iloc[-1]
    prev  = df.iloc[-2]
    prev2 = df.iloc[-3]
    body  = abs(c["close"] - c["open"])
    rng   = c["high"] - c["low"]
    upper = c["high"] - max(c["close"], c["open"])
    lower = min(c["close"], c["open"]) - c["low"]
    if rng > 0:
        if (prev["close"] < prev["open"] and c["close"] > c["open"] and
                c["open"] <= prev["close"] and c["close"] >= prev["open"] and
                body > abs(prev["close"] - prev["open"])):
            p["bull_engulf"] = True
        if (prev["close"] > prev["open"] and c["close"] < c["open"] and
                c["open"] >= prev["close"] and c["close"] <= prev["open"] and
                body > abs(prev["close"] - prev["open"])):
            p["bear_engulf"] = True
        if lower > body * 2.5 and upper < body * 0.5 and body > 0:
            p["hammer"] = True
        if upper > body * 2.5 and lower < body * 0.5 and body > 0:
            p["shooting_star"] = True
        if lower > rng * 0.65 and body < rng * 0.25:
            p["pin_bull"] = True
        if upper > rng * 0.65 and body < rng * 0.25:
            p["pin_bear"] = True
    # Morning/Evening Star
    prev2_body = abs(prev2["close"] - prev2["open"])
    prev_body  = abs(prev["close"]  - prev["open"])
    cur_body   = abs(c["close"]     - c["open"])
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

    if any(v is None or (isinstance(v, float) and np.isnan(v))
           for v in [rsi, macdh, ema9, ema21, ema50, atr]):
        return None

    # Divergencias RSI
    rsi_vals   = df["RSI_14"].tail(10).dropna().values
    close_vals = df["close"].tail(10).values
    bull_div = (len(rsi_vals) >= 5 and
                close_vals[-1] < close_vals[-5] and rsi_vals[-1] > rsi_vals[-5])
    bear_div = (len(rsi_vals) >= 5 and
                close_vals[-1] > close_vals[-5] and rsi_vals[-1] < rsi_vals[-5])

    can     = detect_candles(df)
    ob      = detect_ob(df)
    sr      = detect_sr(df)
    fib     = compute_fib(df)
    vp      = compute_vp(df)
    funding = fetch_funding(symbol)

    vol_spike  = bool(vol20 and vol > vol20 * 1.8)
    vol_climax = bool(vol20 and vol > vol20 * 3.0)

    trend_bull = (ema9 > ema21 > ema50) and (price > ema50)
    trend_bear = (ema9 < ema21 < ema50) and (price < ema50)
    if ema200 and not np.isnan(ema200):
        trend_bull = trend_bull and price > ema200
        trend_bear = trend_bear and price < ema200

    near_bb_lo = bool(bb_lo and abs(price - bb_lo) / price < 0.003)
    near_bb_up = bool(bb_up and abs(price - bb_up) / price < 0.003)

    # ── CONFLUENCIAS LONG ──
    long_c = []
    if trend_bull:            long_c.append("✅ Tendencia alcista EMA9>21>50")
    if rsi < 35:              long_c.append(f"📉 RSI {rsi:.1f} sobrevendido")
    elif rsi < 45:            long_c.append(f"📊 RSI {rsi:.1f} zona de compra")
    if macdh > 0 and macdh_p <= 0:  long_c.append("🔀 MACD cruce alcista")
    elif macdh > 0 and macdh > prev.get("MACDh_12_26_9", 0):
                              long_c.append("📈 MACD histograma creciendo")
    if bull_div:              long_c.append("🔁 Divergencia alcista RSI")
    if ob["bull_ob"]:         long_c.append(f"🧱 Order Block alcista {ob['bull_ob']['low']:.2f}-{ob['bull_ob']['high']:.2f}")
    if sr["support"] and abs(price - sr["support"]) / price < 0.006:
                              long_c.append(f"🛡️ Soporte clave {sr['support']:.2f}")
    if fib["near_level"] in ("0.618","0.786","1.0"):
                              long_c.append(f"🌀 Fibonacci {fib['near_level']}")
    if can["morning_star"]:   long_c.append("🌅 Morning Star")
    elif can["bull_engulf"]:  long_c.append("🕯️ Engulfing alcista")
    elif can["hammer"]:       long_c.append("🔨 Hammer")
    elif can["pin_bull"]:     long_c.append("📌 Pin Bar alcista")
    if near_bb_lo:            long_c.append("📊 Precio en banda inferior BB")
    if vol_climax:            long_c.append("🔥 Volumen climático")
    elif vol_spike:           long_c.append("📊 Spike de volumen")
    if funding and funding < -0.0005:
                              long_c.append(f"💰 Funding negativo {funding*100:.4f}%")
    if vp["val"] and abs(price - vp["val"]) / price < 0.005:
                              long_c.append(f"🎯 Precio en VAL {vp['val']:.2f}")

    # ── CONFLUENCIAS SHORT ──
    short_c = []
    if trend_bear:            short_c.append("✅ Tendencia bajista EMA9<21<50")
    if rsi > 65:              short_c.append(f"📈 RSI {rsi:.1f} sobrecomprado")
    elif rsi > 55:            short_c.append(f"📊 RSI {rsi:.1f} zona de venta")
    if macdh < 0 and macdh_p >= 0:  short_c.append("🔀 MACD cruce bajista")
    elif macdh < 0 and macdh < prev.get("MACDh_12_26_9", 0):
                              short_c.append("📉 MACD histograma cayendo")
    if bear_div:              short_c.append("🔁 Divergencia bajista RSI")
    if ob["bear_ob"]:         short_c.append(f"🧱 Order Block bajista {ob['bear_ob']['low']:.2f}-{ob['bear_ob']['high']:.2f}")
    if sr["resistance"] and abs(price - sr["resistance"]) / price < 0.006:
                              short_c.append(f"🚧 Resistencia clave {sr['resistance']:.2f}")
    if fib["near_level"] in ("0.0","0.236","0.382"):
                              short_c.append(f"🌀 Fibonacci {fib['near_level']}")
    if can["evening_star"]:   short_c.append("🌆 Evening Star")
    elif can["bear_engulf"]:  short_c.append("🕯️ Engulfing bajista")
    elif can["shooting_star"]:short_c.append("💫 Shooting Star")
    elif can["pin_bear"]:     short_c.append("📌 Pin Bar bajista")
    if near_bb_up:            short_c.append("📊 Precio en banda superior BB")
    if vol_climax:            short_c.append("🔥 Volumen climático")
    elif vol_spike:           short_c.append("📊 Spike de volumen")
    if funding and funding > 0.0005:
                              short_c.append(f"💰 Funding positivo {funding*100:.4f}%")
    if vp["vah"] and abs(price - vp["vah"]) / price < 0.005:
                              short_c.append(f"🎯 Precio en VAH {vp['vah']:.2f}")

    # ── DECISIÓN ──
    if len(long_c) >= len(short_c) and len(long_c) >= CONFIG["min_confluences"]:
        direction, confluences = "LONG", long_c
    elif len(short_c) > len(long_c) and len(short_c) >= CONFIG["min_confluences"]:
        direction, confluences = "SHORT", short_c
    else:
        log.info(f"  📈 LONG {len(long_c)} | 📉 SHORT {len(short_c)} — sin señal")
        return None

    # ── SL Y TP ──
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

    return {
        "timestamp":         datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
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
        "stats":             get_stats(symbol),
        "rsi":               round(rsi, 1),
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
{vp_text if vp_text else "  Sin datos\n"}
{hist}

💪 Fuerza: {sig["strength"]}
⚠️ Opera siempre con stop loss y gestión de riesgo."""

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

# ══════════════════════════════════════════════════════
#  🌐  EXPORTAR AL DASHBOARD
# ══════════════════════════════════════════════════════
def export_to_web(new_signal=None):
    global recent_signals_cache
    try:
        if new_signal:
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
            })
            recent_signals_cache = recent_signals_cache[:10]

        data = {
            "last_update":     datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "bot_status":      "ACTIVO",
            "signals_today":   len(recent_signals_cache),
            "pairs_monitored": CONFIG["symbols"],
            "recent_signals":  recent_signals_cache,
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
def should_send(symbol, timeframe):
    key  = f"{symbol}_{timeframe}"
    last = last_signal_time.get(key)
    if last is None: return True
    return (datetime.now(timezone.utc) - last).total_seconds() / 60 >= CONFIG["cooldown_minutes"]

# ══════════════════════════════════════════════════════
#  🚀  LOOP PRINCIPAL
# ══════════════════════════════════════════════════════
def run():
    log.info("🚀 MARCO TRADING BOT v3 INICIADO")
    log.info(f"📊 Pares: {CONFIG['symbols']} | Timeframes: {CONFIG['timeframes']}")
    log.info(f"🎯 Min confluencias: {CONFIG['min_confluences']} | TP1 ratio: {CONFIG['min_rr_tp1']} | TP2 ratio: {CONFIG['min_rr_tp2']}")
    init_db()
    send_telegram("🤖 Marco Trading Bot v3 iniciado\n✅ SL inteligente activo\n🎯 Ratio mínimo TP1: 2:1 | TP2: 4:1\n📊 Solo señales de calidad")
    export_to_web()

    while True:
        for symbol in CONFIG["symbols"]:
            for tf in CONFIG["timeframes"]:
                try:
                    log.info(f"🔍 Analizando {symbol} {tf}m...")
                    df = fetch_candles(symbol, tf, limit=300)
                    if df is None or len(df) < 100: continue
                    signal = analyze(df, symbol, tf)
                    if signal:
                        log.info(f"🚨 SEÑAL {signal['direction']} {symbol} {tf}m | TP1:{signal['rr_tp1']} TP2:{signal['rr_tp2']} | {signal['confluences']} confluencias")
                        if should_send(symbol, tf):
                            send_telegram(build_message(signal))
                            save_signal(signal)
                            export_to_web(signal)
                            last_signal_time[f"{symbol}_{tf}"] = datetime.now(timezone.utc)
                        else:
                            log.info(f"⏸️ Cooldown activo {symbol} {tf}m")
                    else:
                        log.info(f"⏳ Sin señal — {symbol} {tf}m")
                    time.sleep(2)
                except Exception as e:
                    log.error(f"❌ Error {symbol} {tf}m: {e}")
        log.info(f"💤 Próximo análisis en {CONFIG['check_interval']}s")
        time.sleep(CONFIG["check_interval"])

if __name__ == "__main__":
    run()
