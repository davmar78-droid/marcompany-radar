# 🤖 Trading Bot — Alertas Futuros Crypto por WhatsApp
## 100% Gratuito | Bybit API + CallMeBot

---

## ✅ QUÉ ANALIZA

- **RSI** — sobrecompra/sobreventa
- **MACD** — cruces de señal
- **EMA 9/21/50** — tendencia
- **Order Blocks** — zonas de demanda/oferta institucional
- **Soportes y Resistencias** — pivots automáticos
- **Fibonacci** — niveles 0.236, 0.382, 0.5, 0.618, 0.786, 1.618
- **Velas gatillo** — Engulfing, Hammer, Shooting Star, Pin Bar, Doji
- **Volumen** — spikes de volumen
- **Volume Profile** — POC, VAH, VAL
- **Funding Rate** — señal de reversión

## 📲 QUÉ RECIBES EN WHATSAPP

```
🚨 SEÑAL — BTCUSDT 15m
⏰ 2025-04-05 14:32 UTC

📍 Entrada: $94,250.00
🎯 Dirección: 🔴 SHORT

━━━━━━━━━━━━━━━━━━━━
🛡️ SL:  $94,850.00  (-0.64%)

🎯 TP1: $93,200.00  (+1.11%)
   Ratio: 1.7:1

🎯 TP2: $92,100.00  (+2.28%)
   Ratio: 3.6:1 ✅
━━━━━━━━━━━━━━━━━━━━

🧠 CONFLUENCIAS (5/10):
  ✅ RSI 71.3 sobrecomprado
  ✅ MACD cruce bajista
  ✅ Engulfing bajista
  ✅ Order Block bajista 94100-94450
  ✅ Funding positivo 0.0120%

━━━━━━━━━━━━━━━━━━━━
📊 VOLUME PROFILE:
  POC: $93,100.00
  VAH: $94,500.00
  VAL: $92,800.00

━━━━━━━━━━━━━━━━━━━━
📈 HISTORIAL BTCUSDT:
  Señales: 24 | TP1: 67% | TP2: 41%

⚡ Fuerza: ⚡ ALTA
⚠️ Opera siempre con gestión de riesgo.
```

---

## 🚀 INSTALACIÓN (5 minutos)

### Paso 1 — Instalar Python
Descarga Python 3.10+ desde https://python.org

### Paso 2 — Instalar dependencias
Abre una terminal (cmd o PowerShell en Windows) en la carpeta del bot:

```bash
pip install requests pandas pandas-ta numpy
```

### Paso 3 — Activar CallMeBot WhatsApp (GRATIS)

1. Añade este número a tus contactos de WhatsApp:
   **+34 644 66 32 62** (puedes llamarlo "CallMeBot")

2. Envíale este mensaje por WhatsApp:
   ```
   I allow callmebot to send me messages
   ```

3. En 2 minutos recibirás un mensaje con tu **APIKEY** (un número de 6 cifras)

4. Guarda ese número, lo necesitas en el siguiente paso

### Paso 4 — Configurar el bot

Abre `bot.py` con cualquier editor de texto (Notepad, VSCode...) y edita:

```python
CONFIG = {
    "whatsapp_phone":   "+34612345678",  # ← Tu número con prefijo país
    "callmebot_apikey": "123456",        # ← El número que te llegó por WhatsApp
    ...
}
```

### Paso 5 — Ejecutar

```bash
python bot.py
```

¡Listo! El bot analizará los mercados cada 60 segundos y te avisará cuando detecte una señal.

---

## ⚙️ PERSONALIZACIÓN

### Cambiar pares monitorizados
```python
"symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"],
```

### Cambiar timeframes
```python
"timeframes": ["5", "15", "60", "240"],  # 5m, 15m, 1h, 4h
```

### Ajustar filtros de calidad
```python
"min_rr_ratio":    3.0,   # Ratio mínimo (3 = ratio 3:1)
"min_confluences": 4,     # Mínimo de confirmaciones para enviar
"cooldown_minutes": 60,   # Tiempo entre señales del mismo par
```

### Agregar más pares
Cualquier par de futuros de Bybit funciona:
`BNBUSDT`, `DOGEUSDT`, `AVAXUSDT`, `LINKUSDT`, `MATICUSDT`...

---

## 🗄️ HISTORIAL DE SEÑALES

Todas las señales se guardan en `signals.db` (SQLite).
Puedes abrirlo con [DB Browser for SQLite](https://sqlitebrowser.org/) (gratis).

Para ver el historial, instala DB Browser, abre `signals.db` y ve a la tabla `signals`.

Cuando una operación termina, actualiza el campo `result` con:
- `TP1` — si llegó al primer objetivo
- `TP2` — si llegó al segundo objetivo
- `SL`  — si tocó el stop loss

Así el bot aprende el % de acierto real de tus señales.

---

## 📋 ARCHIVOS

```
trading_bot/
├── bot.py          ← El bot principal
├── README.md       ← Este archivo
├── requirements.txt← Dependencias
├── bot.log         ← Log de actividad (se crea al correr)
└── signals.db      ← Historial de señales (se crea al correr)
```

---

## ⚠️ AVISO LEGAL

Este bot es una **herramienta de análisis técnico**, no una recomendación financiera.
El trading de futuros conlleva riesgo de pérdida del capital.
Usa siempre stop loss y gestión de riesgo adecuada.
Nunca arriesgues más del 1-2% de tu capital por operación.
