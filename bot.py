import requests
import pandas as pd
import pandas_ta as ta
import numpy as np
import json, time, logging, sqlite3, urllib.parse
from datetime import datetime, timezone

CONFIG = {
    "telegram_token":  "8135742976:AAHK6NPEYrb90IGGj764RqCoqLXVIPywgBU",
    "telegram_chats":  ["-1003893933581", "772021739"],
    "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    "timeframes": ["15", "60"],
    "min_rr_ratio": 3.0,
    "min_confluences": 4,
    "check_interval": 60,
    "cooldown_minutes": 60,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("bot.log", encoding="utf-8"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)
last_signal_time = {}

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
    log.info("Base de datos lista.")

def save_signal(sig):
    conn = sqlite3.connect("signals.db")
    conn.cursor().execute("""
        INSERT INTO signals (timestamp,symbol,timeframe,direction,entry_price,
        sl,tp1,tp2,rr_tp1,rr_tp2,confluences,confluence_detail,strength)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (sig["timestamp"],sig["symbol"],sig["timeframe"],sig["direction"],
          sig["entry_price"],sig["sl"],sig["tp1"],sig["tp2"],
          sig["rr_tp1"],sig["rr_tp2"],sig["confluences"],
          json.dumps(sig["confluence_detail"]),sig["strength"]))
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
    return {"total": total,
            "win_rate_tp1": round(sum(1 for r in rows if r[14] in ("TP1","TP2"))/total*100,1),
            "win_rate_tp2": round(sum(1 for r in rows if r[14]=="TP2")/total*100,1)}

def fetch_candles(symbol, interval, limit=200):
    try:
        r = requests.get("https://api.bybit.com/v5/market/kline",
            params={"category":"linear","symbol":symbol,"interval":interval,"limit":limit}, timeout=10)
        data = r.json()
        if data["retCode"] != 0: return None
        df = pd.DataFrame(data["result"]["list"],
            columns=["timestamp","open","high","low","close","volume","turnover"])
        df = df.astype({"timestamp":"int64","open":"float64","high":"float64",
                        "low":"float64","close":"float64","volume":"float64"})
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        return df.sort_values("timestamp").reset_index(drop=True)
    except Exception as e:
        log.error(f"Error {symbol}: {e}"); return None

def fetch_funding(symbol):
    try:
        r = requests.get("https://api.bybit.com/v5/market/tickers",
            params={"category":"linear","symbol":symbol}, timeout=10)
        items = r.json()["result"]["list"]
        if items: return float(items[0].get("fundingRate", 0))
    except: pass
    return None

def compute_indicators(df):
    df.ta.rsi(length=14, append=True)
    df.ta.macd(fast=12, slow=26, signal=9, append=True)
    df.ta.ema(length=9, append=True)
    df.ta.ema(length=21, append=True)
    df.ta.atr(length=14, append=True)
    df["vol_sma20"] = df["volume"].rolling(20).mean()
    return df

def detect_candles(df):
    p = {"bull_engulf":False,"bear_engulf":False,"hammer":False,
         "shooting_star":False,"pin_bull":False,"pin_bear":False}
    if len(df) < 3: return p
    c, prev = df.iloc[-1], df.iloc[-2]
    body = abs(c["close"]-c["open"])
    rng = c["high"]-c["low"]
    upper = c["high"]-max(c["close"],c["open"])
    lower = min(c["close"],c["open"])-c["low"]
    if prev["close"]<prev["open"] and c["close"]>c["open"] and c["open"]<prev["close"] and c["close"]>prev["open"]:
        p["bull_engulf"]=True
    if prev["close"]>prev["open"] and c["close"]<c["open"] and c["open"]>prev["close"] and c["close"]<prev["open"]:
        p["bear_engulf"]=True
    if rng>0 and lower>body*2 and upper<body*0.5: p["hammer"]=True
    if rng>0 and upper>body*2 and lower<body*0.5: p["shooting_star"]=True
    if rng>0 and lower>rng*0.6 and body<rng*0.3: p["pin_bull"]=True
    if rng>0 and upper>rng*0.6 and body<rng*0.3: p["pin_bear"]=True
    return p

def detect_ob(df):
    result = {"bull_ob":None,"bear_ob":None}
    if len(df)<30: return result
    sub = df.tail(30).reset_index(drop=True)
    price = sub.iloc[-1]["close"]
    for i in range(len(sub)-3,2,-1):
        c=sub.iloc[i]
        if c["close"]>c["open"]:
            if sub.iloc[i+1]["close"]-sub.iloc[i+1]["open"] < -abs(c["close"]-c["open"])*1.5:
                if c["low"]<=price<=c["high"]*1.002:
                    result["bear_ob"]={"high":c["high"],"low":c["low"]}; break
    for i in range(len(sub)-3,2,-1):
        c=sub.iloc[i]
        if c["close"]<c["open"]:
            if sub.iloc[i+1]["close"]-sub.iloc[i+1]["open"] > abs(c["close"]-c["open"])*1.5:
                if c["low"]*0.998<=price<=c["high"]:
                    result["bull_ob"]={"high":c["high"],"low":c["low"]}; break
    return result

def detect_sr(df):
    result={"resistance":None,"support":None}
    if len(df)<50: return result
    sub=df.tail(50); price=df.iloc[-1]["close"]; levels=[]
    for i in range(2,len(sub)-2):
        h=sub.iloc[i]["high"]; l=sub.iloc[i]["low"]
        if h>sub.iloc[i-1]["high"] and h>sub.iloc[i-2]["high"] and h>sub.iloc[i+1]["high"] and h>sub.iloc[i+2]["high"]:
            levels.append(("R",h))
        if l<sub.iloc[i-1]["low"] and l<sub.iloc[i-2]["low"] and l<sub.iloc[i+1]["low"] and l<sub.iloc[i+2]["low"]:
            levels.append(("S",l))
    res=[v for t,v in levels if t=="R" and v>price*1.003]
    sup=[v for t,v in levels if t=="S" and v<price*0.997]
    if res: result["resistance"]=min(res)
    if sup: result["support"]=max(sup)
    return result

def compute_fib(df):
    sub=df.tail(50); sh=sub["high"].max(); sl=sub["low"].min(); d=sh-sl
    price=df.iloc[-1]["close"]
    levels={"0.0":sh,"0.236":sh-d*0.236,"0.382":sh-d*0.382,"0.5":sh-d*0.5,
            "0.618":sh-d*0.618,"0.786":sh-d*0.786,"1.0":sl,"1.618":sl-d*0.618}
    near=next((k for k,v in levels.items() if abs(price-v)/price<0.003),None)
    return {"levels":levels,"near_level":near}

def compute_vp(df,bins=20):
    if len(df)<20: return {"poc":None,"vah":None,"val":None}
    lo=df["low"].min(); hi=df["high"].max(); bin_sz=(hi-lo)/bins; profile={}
    for _,row in df.iterrows():
        n=max(1,int((row["high"]-row["low"])/bin_sz)); vpb=row["volume"]/n
        for b in range(n+1):
            pl=row["low"]+b*bin_sz; bk=round(lo+int((pl-lo)/bin_sz)*bin_sz,2)
            profile[bk]=profile.get(bk,0)+vpb
    if not profile: return {"poc":None,"vah":None,"val":None}
    sp=sorted(profile.items(),key=lambda x:x[1],reverse=True); poc=sp[0][0]
    total=sum(profile.values()); acc=0; va=[]
    for bk,v in sp:
        va.append(bk); acc+=v
        if acc>=total*0.7: break
    return {"poc":poc,"vah":max(va) if va else None,"val":min(va) if va else None}

def analyze(df,symbol,timeframe):
    if len(df)<60: return None
    df=compute_indicators(df)
    last=df.iloc[-1]; prev=df.iloc[-2]; price=last["close"]
    rsi=last.get("RSI_14"); macdh=last.get("MACDh_12_26_9")
    macdh_p=prev.get("MACDh_12_26_9"); ema9=last.get("EMA_9")
    ema21=last.get("EMA_21"); atr=last.get("ATRr_14")
    vol=last.get("volume"); vol_avg=last.get("vol_sma20")
    if any(v is None or (isinstance(v,float) and np.isnan(v)) for v in [rsi,macdh,ema9,ema21,atr]):
        return None
    can=detect_candles(df); ob=detect_ob(df); sr=detect_sr(df)
    fib=compute_fib(df); vp=compute_vp(df); funding=fetch_funding(symbol)
    vol_spike=bool(vol_avg and vol>vol_avg*1.5)

    long_c=[]
    if rsi<35: long_c.append(f"RSI {rsi:.1f} sobrevendido")
    if macdh>0 and macdh_p<0: long_c.append("MACD cruce alcista")
    if ema9>ema21: long_c.append("EMA9 > EMA21 alcista")
    if can["bull_engulf"]: long_c.append("Engulfing alcista")
    elif can["hammer"]: long_c.append("Hammer")
    elif can["pin_bull"]: long_c.append("Pin Bar alcista")
    if ob["bull_ob"]: long_c.append(f"Order Block alcista {ob['bull_ob']['low']:.1f}-{ob['bull_ob']['high']:.1f}")
    if sr["support"] and abs(price-sr["support"])/price<0.005: long_c.append(f"Soporte {sr['support']:.1f}")
    if fib["near_level"] in ("0.618","0.786","1.0"): long_c.append(f"Fibonacci {fib['near_level']}")
    if vol_spike: long_c.append("Spike de volumen")
    if funding and funding<-0.0005: long_c.append(f"Funding negativo {funding*100:.4f}%")

    short_c=[]
    if rsi>65: short_c.append(f"RSI {rsi:.1f} sobrecomprado")
    if macdh<0 and macdh_p>0: short_c.append("MACD cruce bajista")
    if ema9<ema21: short_c.append("EMA9 < EMA21 bajista")
    if can["bear_engulf"]: short_c.append("Engulfing bajista")
    elif can["shooting_star"]: short_c.append("Shooting Star")
    elif can["pin_bear"]: short_c.append("Pin Bar bajista")
    if ob["bear_ob"]: short_c.append(f"Order Block bajista {ob['bear_ob']['low']:.1f}-{ob['bear_ob']['high']:.1f}")
    if sr["resistance"] and abs(price-sr["resistance"])/price<0.005: short_c.append(f"Resistencia {sr['resistance']:.1f}")
    if fib["near_level"] in ("0.0","0.236","0.382"): short_c.append(f"Fibonacci {fib['near_level']}")
    if vol_spike: short_c.append("Spike de volumen")
    if funding and funding>0.0005: short_c.append(f"Funding positivo {funding*100:.4f}%")

    if len(long_c)>=len(short_c) and len(long_c)>=CONFIG["min_confluences"]:
        direction,confluences="LONG",long_c
    elif len(short_c)>len(long_c) and len(short_c)>=CONFIG["min_confluences"]:
        direction,confluences="SHORT",short_c
    else:
        return None

    atr_sl=atr*1.5
    if direction=="LONG":
        sl=min(df.tail(10)["low"].min()-atr*0.3, price-atr_sl)
        tp1=sr["resistance"] if sr["resistance"] else price+(price-sl)*1.5
        tp2=fib["levels"].get("1.618") or price+(price-sl)*3.5
        if tp2<tp1: tp2=price+(price-sl)*3.5
    else:
        sl=max(df.tail(10)["high"].max()+atr*0.3, price+atr_sl)
        tp1=sr["support"] if sr["support"] else price-(sl-price)*1.5
        tp2=fib["levels"].get("1.618") or price-(sl-price)*3.5
        if tp2>tp1: tp2=price-(sl-price)*3.5

    risk=abs(price-sl)
    rr1=round(abs(tp1-price)/risk,2) if risk>0 else 0
    rr2=round(abs(tp2-price)/risk,2) if risk>0 else 0
    if rr2<CONFIG["min_rr_ratio"]:
        log.info(f"Descartada {direction} {symbol} R/R {rr2:.1f}"); return None

    n=len(confluences)
    strength="Muy alta" if n>=7 else ("Alta" if n>=5 else ("Media" if n>=4 else "Baja"))
    return {"timestamp":datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            "symbol":symbol,"timeframe":f"{timeframe}m","direction":direction,
            "entry_price":round(price,4),"sl":round(sl,4),"tp1":round(tp1,4),"tp2":round(tp2,4),
            "rr_tp1":rr1,"rr_tp2":rr2,"confluences":n,"confluence_detail":confluences,
            "strength":strength,"poc":vp.get("poc"),"vah":vp.get("vah"),"val":vp.get("val"),
            "funding":funding,"stats":get_stats(symbol),"rsi":round(rsi,1)}

def build_message(sig):
    sl_pct=abs(sig["entry_price"]-sig["sl"])/sig["entry_price"]*100
    tp1_pct=abs(sig["tp1"]-sig["entry_price"])/sig["entry_price"]*100
    tp2_pct=abs(sig["tp2"]-sig["entry_price"])/sig["entry_price"]*100
    stats=sig.get("stats",{}); poc=sig.get("poc"); vah=sig.get("vah"); val=sig.get("val")
    conf="\n".join(f"  - {c}" for c in sig["confluence_detail"])
    vp=""
    if poc: vp+=f"  POC: ${poc:,.2f}\n"
    if vah: vp+=f"  VAH: ${vah:,.2f}\n"
    if val: vp+=f"  VAL: ${val:,.2f}\n"
    return f"""SENAL - {sig["symbol"]} {sig["timeframe"]}
{sig["timestamp"]}

Entrada: ${sig["entry_price"]:,.2f}
Direccion: {sig["direction"]}

SL:  ${sig["sl"]:,.2f}  (-{sl_pct:.2f}%)
TP1: ${sig["tp1"]:,.2f}  (+{tp1_pct:.2f}%)  Ratio: {sig["rr_tp1"]}:1
TP2: ${sig["tp2"]:,.2f}  (+{tp2_pct:.2f}%)  Ratio: {sig["rr_tp2"]}:1

CONFLUENCIAS ({sig["confluences"]}):
{conf}

VOLUME PROFILE:
{vp}
HISTORIAL: {stats.get("total",0)} senales | TP1: {stats.get("win_rate_tp1",0)}% | TP2: {stats.get("win_rate_tp2",0)}%
Fuerza: {sig["strength"]}
Opera siempre con stop loss."""

def send_telegram(message):
    phone=CONFIG["whatsapp_phone"]; apikey=CONFIG["callmebot_apikey"]
    if apikey=="TU_API_KEY":
        log.warning("Pon tu APIKEY de CallMeBot en CONFIG"); print(message); return
    url=f"https://api.callmebot.com/whatsapp.php?phone={phone}&text={urllib.parse.quote(message)}&apikey={apikey}"
    try:
        r=requests.get(url,timeout=15)
        log.info("WhatsApp enviado." if r.status_code==200 else f"Error {r.status_code}")
    except Exception as e: log.error(f"Error WhatsApp: {e}")

def should_send(symbol,timeframe):
    key=f"{symbol}_{timeframe}"; last=last_signal_time.get(key)
    if last is None: return True
    return (datetime.now(timezone.utc)-last).total_seconds()/60>=CONFIG["cooldown_minutes"]

def run():
    log.info("TRADING BOT INICIADO"); log.info(f"Pares: {CONFIG['symbols']}")
    init_db()
    while True:
        for symbol in CONFIG["symbols"]:
            for tf in CONFIG["timeframes"]:
                try:
                    log.info(f"Analizando {symbol} {tf}m...")
                    df=fetch_candles(symbol,tf,limit=200)
                    if df is None or len(df)<60: continue
                    signal=analyze(df,symbol,tf)
                    if signal:
                        log.info(f"SENAL {signal['direction']} {symbol} {tf}m R/R:{signal['rr_tp2']}")
                        if should_send(symbol,tf):
                            send_telegram(build_message(signal))
                            save_signal(signal)
                            last_signal_time[f"{symbol}_{tf}"]=datetime.now(timezone.utc)
                    else:
                        log.info(f"Sin senal - {symbol} {tf}m")
                    time.sleep(2)
                except Exception as e: log.error(f"Error {symbol} {tf}m: {e}")
        log.info(f"Proximo analisis en {CONFIG['check_interval']}s")
        time.sleep(CONFIG["check_interval"])

if __name__=="__main__":
    run()
