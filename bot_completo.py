import requests, json, pandas as pd, pandas_ta as ta, numpy as np, time, logging
from datetime import datetime, timezone

CONFIG = {
    "telegram_token": "8135742976:AAHK6NPEYrb90IGGj764RqCoqLXVIPywgBU", 
    "telegram_chats": ["-1003893933581", "772021739"],
    "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"],
    "timeframes": ["15", "60", "240", "D"],
    "min_rr_ratio": 2.5,
    "min_confluences": 2,
    "cooldown_minutes": 5,
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
                log.info(f"✅ Telegram {chat} OK")
            else:
                log.warning(f"❌ Error {chat}: {r.status_code}")
        except Exception as e:
            log.error(f"Error {chat}: {e}")

def fetch_candles(symbol, timeframe, limit=200):
    try:
        url = f"https://fapi.binance.com/fapi/v1/klines"
        params = {"symbol": symbol, "interval": f"{timeframe}m" if timeframe != "D" else "1d", "limit": limit}
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200: return None
        
        data = r.json()
        df = pd.DataFrame(data, columns=["timestamp","open","high","low","close","volume","close_time","quote_asset_volume","number_of_trades","taker_buy_base_asset_volume","taker_buy_quote_asset_volume","ignore"])
        df = df[["timestamp","open","high","low","close","volume"]].astype(float)
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
        return df
    except Exception as e:
        log.error(f"Error fetch {symbol}: {e}")
        return None

def should_send(symbol, tf):
    key = f"{symbol}_{tf}"
    last = last_signal_time.get(key)
    if last is None: return True
    return (datetime.now(timezone.utc) - last).total_seconds() / 60 >= CONFIG["cooldown_minutes"]

def analyze(df, symbol, tf):
    if len(df) < 50: return None
    
    # Indicadores básicos
    df.ta.rsi(length=14, append=True)
    df.ta.macd(fast=12, slow=26, signal=9, append=True)
    df.ta.ema(length=9, append=True)
    df.ta.ema(length=21, append=True)
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    rsi = last.get("RSI_14")
    macdh = last.get("MACDh_12_26_9")
    macdh_p = prev.get("MACDh_12_26_9")
    ema9 = last.get("EMA_9")
    ema21 = last.get("EMA_21")
    
    if any(v is None or np.isnan(v) for v in [rsi, macdh, ema9, ema21]):
        return None
    
    price = last["close"]
    
    # Lógica de señales simplificada
    confluences = []
    direction = None
    
    # Señales LONG
    if rsi < 35:
        confluences.append(f"RSI {rsi:.1f} sobrevendido")
    if macdh > 0 and macdh_p < 0:
        confluences.append("MACD cruce alcista")
    if ema9 > ema21:
        confluences.append("EMA9 > EMA21")
    
    if len(confluences) >= 2:
        direction = "LONG"
        sl = price * 0.98
        tp1 = price * 1.02
        tp2 = price * 1.04
    
    # Señales SHORT
    confluences_short = []
    if rsi > 65:
        confluences_short.append(f"RSI {rsi:.1f} sobrecomprado")
    if macdh < 0 and macdh_p > 0:
        confluences_short.append("MACD cruce bajista")
    if ema9 < ema21:
        confluences_short.append("EMA9 < EMA21")
    
    if len(confluences_short) >= 2 and direction is None:
        direction = "SHORT"
        confluences = confluences_short
        sl = price * 1.02
        tp1 = price * 0.98
        tp2 = price * 0.96
    
    if direction and len(confluences) >= CONFIG["min_confluences"]:
        rr_tp1 = abs(tp1 - price) / abs(price - sl)
        rr_tp2 = abs(tp2 - price) / abs(price - sl)
        
        if rr_tp2 >= CONFIG["min_rr_ratio"]:
            return {
                "symbol": symbol,
                "timeframe": tf,
                "direction": direction,
                "entry_price": price,
                "sl": sl,
                "tp1": tp1,
                "tp2": tp2,
                "rr_tp2": rr_tp2,
                "confluences": len(confluences),
                "confluence_detail": confluences
            }
    
    return None

def build_message(signal):
    sl_pct = abs(signal["entry_price"] - signal["sl"]) / signal["entry_price"] * 100
    tp2_pct = abs(signal["tp2"] - signal["entry_price"]) / signal["entry_price"] * 100
    conf_text = "\n".join(f"- {c}" for c in signal["confluence_detail"])
    
    return f"""SENAL {signal["direction"]} DETECTADA
{signal["symbol"]} {signal["timeframe"]}

Entrada: ${signal["entry_price"]:,.2f}
SL: ${signal["sl"]:,.2f} (-{sl_pct:.1f}%)
TP2: ${signal["tp2"]:,.2f} (+{tp2_pct:.1f}%) R/R: {signal["rr_tp2"]:.1f}:1

CONFLUENCIAS ({signal["confluences"]}):
{conf_text}

Opera con stop loss"""



def export_to_web():
    try:
        import json, subprocess
        from datetime import datetime
        signals_data = {
            "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "bot_status": "ACTIVO", 
            "signals_today": 0,
            "pairs_monitored": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"],
            "recent_signals": []
        }
        
        with open("signals_live.json", "w") as f:
            json.dump(signals_data, f, indent=2)
        
        # Auto push a GitHub
        subprocess.run(["git", "add", "signals_live.json"], capture_output=True)
        subprocess.run(["git", "commit", "-m", "Update live data"], capture_output=True)
        subprocess.run(["git", "push"], capture_output=True)
        
        log.info("📊 Dashboard + GitHub actualizados")
    except Exception as e:
        log.error(f"Error export: {e}")

def run():
    log.info("🚀 Marco Bot Completo iniciado")
    send_telegram("Marco Bot iniciado - Monitorizando señales")
    
    while True:
        for symbol in CONFIG["symbols"]:
            for tf in CONFIG["timeframes"]:
                try:
                    log.info(f"Analizando {symbol} {tf}...")
                    df = fetch_candles(symbol, tf)
                    if df is None: continue
                    
                    signal = analyze(df, symbol, tf)
                    if signal:
                        log.info(f"🚨 SEÑAL {signal['direction']} {symbol} {tf} R/R:{signal['rr_tp2']:.1f}")
                        if should_send(symbol, tf):
                            send_telegram(build_message(signal))
                            last_signal_time[f"{symbol}_{tf}"] = datetime.now(timezone.utc)
                        else:
                            log.info(f"⏸️ Cooldown activo {symbol} {tf}")
                    else:
                        log.info(f"Sin señal {symbol} {tf}")
                    
                    time.sleep(1)
                except Exception as e:
                    log.error(f"Error {symbol} {tf}: {e}")
        
        export_to_web()
        log.info("Próximo análisis en 60s")
        time.sleep(60)

if __name__ == "__main__":
    run()
