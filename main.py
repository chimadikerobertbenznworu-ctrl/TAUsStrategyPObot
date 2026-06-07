import asyncio
import os
import time
import json
import websockets
from datetime import datetime
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO, emit
import threading
import requests

app = Flask(__name__)
app.config['SECRET_KEY'] = 'TAUsStrategyPObot2026'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

bot_state = {
    "scanning": True,
    "telegram_alerts": True,
    "sound_alerts": True,
    "main_strategy": True,
    "pattern_strategy": True,
    "setup_warnings": True,
    "connected": False,
    "signals": [],
    "scan_count": 0,
    "last_scan": None,
    "assets_loaded": 0,
    "connection_status": "Disconnected"
}

SSID = os.environ.get("PO_SSID", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
MIN_PAYOUT = int(os.environ.get("MIN_PAYOUT", "80"))

WS_ENDPOINTS = [
    "wss://api-eu.po.market/socket.io/?EIO=4&transport=websocket",
    "wss://api-spb.po.market/socket.io/?EIO=4&transport=websocket",
    "wss://api-msk.po.market/socket.io/?EIO=4&transport=websocket",
    "wss://api-us-north.po.market/socket.io/?EIO=4&transport=websocket",
    "wss://api-us-south.po.market/socket.io/?EIO=4&transport=websocket",
]

KNOWN_OTC_ASSETS = [
    "EURUSD_otc","GBPUSD_otc","USDJPY_otc","USDCHF_otc","USDCAD_otc",
    "AUDUSD_otc","NZDUSD_otc","GBPJPY_otc","EURJPY_otc","EURGBP_otc",
    "AUDJPY_otc","GBPAUD_otc","GBPCAD_otc","GBPCHF_otc","EURCHF_otc",
    "EURAUD_otc","EURCAD_otc","AUDCAD_otc","AUDCHF_otc","AUDNZD_otc",
    "NZDCAD_otc","NZDCHF_otc","NZDJPY_otc","CADJPY_otc","CADCHF_otc",
    "CHFJPY_otc","EURNZD_otc","GBPNZD_otc","USDRUB_otc","USDTRY_otc",
    "BTCUSD_otc","ETHUSD_otc","LTCUSD_otc","XRPUSD_otc","SOLUSD_otc",
    "BNBUSD_otc","DOGUSD_otc","ADAUSD_otc","DOTUSD_otc","LNKUSD_otc",
    "SHIBUSD_otc","UNIUSD_otc","ATOMUSD_otc","ALGOUSD_otc","BNBUSD_otc",
    "#AAPL_otc","#GOOG_otc","#AMZN_otc","#MSFT_otc","#TSLA_otc",
    "#META_otc","#NFLX_otc","#NVDA_otc","#AMD_otc","#INTC_otc",
    "#COIN_otc","#MARA_otc","#PLTR_otc","#GME_otc","#AMC_otc",
    "#BA_otc","#FDX_otc","#DIS_otc","#MCD_otc","#PFE_otc",
    "#AAPL_otc","#SNAP_otc","#UBER_otc","#PYPL_otc","#SQ_otc",
    "XAUUSD_otc","XAGUSD_otc","UKBRENT_otc","USCRUDEOTC",
    "SP500_otc","NASDAQ_otc","DJ30_otc","FTSE100_otc","DAX30_otc",
]

# live candle storage per asset
candle_store = {}

# ── INDICATORS ──────────────────────────────────────────────────────────────

def calculate_ema(prices, period):
    if len(prices) < period:
        return None
    k = 2 / (period + 1)
    ema = sum(prices[:period]) / period
    for price in prices[period:]:
        ema = price * k + ema * (1 - k)
    return ema

def calculate_atr(candles, period):
    if len(candles) < period + 1:
        return None
    true_ranges = []
    for i in range(1, len(candles)):
        tr = max(
            candles[i]['high'] - candles[i]['low'],
            abs(candles[i]['high'] - candles[i-1]['close']),
            abs(candles[i]['low'] - candles[i-1]['close'])
        )
        true_ranges.append(tr)
    if len(true_ranges) < period:
        return None
    return sum(true_ranges[-period:]) / period

def calculate_keltner(candles, ema_period=20, atr_period=10, multiplier=1):
    if len(candles) < max(ema_period, atr_period) + 1:
        return None, None, None
    closes = [c['close'] for c in candles]
    middle = calculate_ema(closes, ema_period)
    atr = calculate_atr(candles, atr_period)
    if middle is None or atr is None:
        return None, None, None
    return middle + multiplier * atr, middle, middle - multiplier * atr

def calculate_stochastic(candles, k_period=14, d_period=3, smooth=3):
    if len(candles) < k_period + d_period + smooth:
        return None, None
    k_values = []
    for i in range(k_period - 1, len(candles)):
        window = candles[i - k_period + 1:i + 1]
        highest = max(c['high'] for c in window)
        lowest = min(c['low'] for c in window)
        close = candles[i]['close']
        k_values.append(50 if highest == lowest else
                        100 * (close - lowest) / (highest - lowest))
    smoothed_k = [sum(k_values[i-smooth+1:i+1])/smooth
                  for i in range(smooth-1, len(k_values))]
    if len(smoothed_k) < d_period:
        return None, None
    d_values = [sum(smoothed_k[i-d_period+1:i+1])/d_period
                for i in range(d_period-1, len(smoothed_k))]
    return smoothed_k[-1], d_values[-1]

def detect_clean_trend(candles, lookback=6):
    if len(candles) < lookback:
        return None
    recent = candles[-lookback:]
    closes = [c['close'] for c in recent]
    bearish = sum(1 for c in recent if c['close'] < c['open'])
    bullish = sum(1 for c in recent if c['close'] > c['open'])
    if bearish >= 4:
        if sum(1 for i in range(1, len(closes))
               if closes[i] < closes[i-1]) >= 4:
            return "downtrend"
    if bullish >= 4:
        if sum(1 for i in range(1, len(closes))
               if closes[i] > closes[i-1]) >= 4:
            return "uptrend"
    return None

# ── MAIN STRATEGY ────────────────────────────────────────────────────────────

def analyze_main_strategy(asset, candles):
    if len(candles) < 30:
        return None
    trend = detect_clean_trend(candles[:-1])
    if not trend:
        return None
    upper_kc, _, lower_kc = calculate_keltner(candles[:-1])
    if upper_kc is None:
        return None
    stoch_k, stoch_d = calculate_stochastic(candles[:-1])
    if stoch_k is None:
        return None
    last = candles[-1]
    prev = candles[-2]
    if trend == "downtrend":
        stoch_ok = stoch_k > stoch_d and stoch_k < 40
        approaching = last['low'] <= lower_kc * 1.001
        c1_valid = (prev['close'] > lower_kc and
                    prev['low'] <= lower_kc * 1.002)
        c2_bull = last['close'] > last['open']
        if stoch_ok and approaching and not c1_valid:
            return {"asset": asset, "direction": "BUY",
                    "strategy": "main", "level": 2,
                    "level_name": "Price Approaching Keltner Zone",
                    "stoch_k": round(stoch_k, 2),
                    "stoch_d": round(stoch_d, 2)}
        elif stoch_ok and c1_valid and not c2_bull:
            return {"asset": asset, "direction": "BUY",
                    "strategy": "main", "level": 3,
                    "level_name": "Candle 1 Closed — Watch Confirmation Candle",
                    "stoch_k": round(stoch_k, 2),
                    "stoch_d": round(stoch_d, 2)}
        elif stoch_ok and c1_valid and c2_bull:
            return {"asset": asset, "direction": "BUY",
                    "strategy": "main", "level": 5,
                    "level_name": "ENTRY SIGNAL",
                    "stoch_k": round(stoch_k, 2),
                    "stoch_d": round(stoch_d, 2),
                    "entry": True}
    elif trend == "uptrend":
        stoch_ok = stoch_k < stoch_d and stoch_k > 60
        approaching = last['high'] >= upper_kc * 0.999
        c1_valid = (prev['close'] < upper_kc and
                    prev['high'] >= upper_kc * 0.998)
        c2_bear = last['close'] < last['open']
        if stoch_ok and approaching and not c1_valid:
            return {"asset": asset, "direction": "SELL",
                    "strategy": "main", "level": 2,
                    "level_name": "Price Approaching Keltner Zone",
                    "stoch_k": round(stoch_k, 2),
                    "stoch_d": round(stoch_d, 2)}
        elif stoch_ok and c1_valid and not c2_bear:
            return {"asset": asset, "direction": "SELL",
                    "strategy": "main", "level": 3,
                    "level_name": "Candle 1 Closed — Watch Confirmation Candle",
                    "stoch_k": round(stoch_k, 2),
                    "stoch_d": round(stoch_d, 2)}
        elif stoch_ok and c1_valid and c2_bear:
            return {"asset": asset, "direction": "SELL",
                    "strategy": "main", "level": 5,
                    "level_name": "ENTRY SIGNAL",
                    "stoch_k": round(stoch_k, 2),
                    "stoch_d": round(stoch_d, 2),
                    "entry": True}
    return None

# ── PATTERN STRATEGY ─────────────────────────────────────────────────────────

def analyze_pattern_strategy(asset, candles):
    if len(candles) < 10:
        return None
    trend = detect_clean_trend(candles[:-2])
    if not trend:
        return None
    c1, c2, c3 = candles[-3], candles[-2], candles[-1]
    c1_body = abs(c1['close'] - c1['open'])
    c2_body = abs(c2['close'] - c2['open'])
    if c1_body == 0:
        return None
    ratio = c2_body / c1_body
    if trend == "uptrend" and c1['close'] > c1['open'] and c2['close'] < c2['open']:
        if ratio >= 0.5:
            return {"asset": asset, "direction": "SELL",
                    "strategy": "pattern",
                    "pattern_type": "Type 1 (Full/Partial Match)",
                    "level": 3,
                    "level_name": "Pattern Confirmed — Enter SELL on Next Candle",
                    "ratio": round(ratio * 100), "entry": True}
        elif 0.05 < ratio < 0.5:
            if c3['close'] < c3['open']:
                return {"asset": asset, "direction": "SELL",
                        "strategy": "pattern",
                        "pattern_type": "Type 2 (Hammer)",
                        "level": 5,
                        "level_name": "Hammer Confirmed — Enter SELL on Next Candle",
                        "ratio": round(ratio * 100), "entry": True}
            else:
                return {"asset": asset, "direction": "SELL",
                        "strategy": "pattern",
                        "pattern_type": "Type 2 (Hammer)",
                        "level": 3,
                        "level_name": "Hammer Candle Confirmed — Watch Next Candle",
                        "ratio": round(ratio * 100), "entry": False}
    elif trend == "downtrend" and c1['close'] < c1['open'] and c2['close'] > c2['open']:
        if ratio >= 0.5:
            return {"asset": asset, "direction": "BUY",
                    "strategy": "pattern",
                    "pattern_type": "Type 1 (Full/Partial Match)",
                    "level": 3,
                    "level_name": "Pattern Confirmed — Enter BUY on Next Candle",
                    "ratio": round(ratio * 100), "entry": True}
        elif 0.05 < ratio < 0.5:
            if c3['close'] > c3['open']:
                return {"asset": asset, "direction": "BUY",
                        "strategy": "pattern",
                        "pattern_type": "Type 2 (Hammer)",
                        "level": 5,
                        "level_name": "Hammer Confirmed — Enter BUY on Next Candle",
                        "ratio": round(ratio * 100), "entry": True}
            else:
                return {"asset": asset, "direction": "BUY",
                        "strategy": "pattern",
                        "pattern_type": "Type 2 (Hammer)",
                        "level": 3,
                        "level_name": "Hammer Candle Confirmed — Watch Next Candle",
                        "ratio": round(ratio * 100), "entry": False}
    return None

# ── TELEGRAM ─────────────────────────────────────────────────────────────────

def send_telegram(message):
    if not bot_state["telegram_alerts"] or not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message,
                  "parse_mode": "HTML"}, timeout=5)
    except Exception as e:
        print(f"Telegram error: {e}")

def format_signal(signal):
    d = "🟢" if signal['direction'] == "BUY" else "🔴"
    e = "✅" if signal.get('entry') else "⚠️"
    s = "Keltner Channel" if signal['strategy'] == "main" else "Pattern Recognition"
    msg = f"{e} <b>{signal['level_name']}</b>\n\n"
    msg += f"{d} <b>{signal['direction']}</b> — {signal['asset']}\n"
    msg += f"📊 Strategy: {s}\n"
    if signal['strategy'] == "pattern":
        msg += f"🕯 Pattern: {signal.get('pattern_type', '')}\n"
        msg += f"📏 Size: {signal.get('ratio', '')}%\n"
    msg += f"⏰ {datetime.now().strftime('%H:%M:%S')} | M5\n"
    if signal.get('entry'):
        msg += f"\n<b>➡️ Enter {signal['direction']} on next candle</b>"
    return msg

# ── SIGNAL PROCESSING ─────────────────────────────────────────────────────────

def process_signal(signal):
    if not signal:
        return
    signal['time'] = datetime.now().strftime('%H:%M:%S')
    signal['id'] = f"{signal['asset']}_{int(time.time())}"
    bot_state['signals'].insert(0, signal)
    bot_state['signals'] = bot_state['signals'][:50]
    socketio.emit('new_signal', signal)
    if signal.get('entry') or signal.get('level', 0) >= 3:
        send_telegram(format_signal(signal))
    print(f"[{signal['time']}] {signal['direction']} "
          f"{signal['asset']} — {signal['level_name']}")

# ── CANDLE ANALYSIS ───────────────────────────────────────────────────────────

def run_analysis(asset):
    candles = candle_store.get(asset, [])
    if len(candles) < 30:
        return
    if bot_state['main_strategy']:
        process_signal(analyze_main_strategy(asset, candles))
    if bot_state['pattern_strategy']:
        process_signal(analyze_pattern_strategy(asset, candles))
    bot_state['scan_count'] += 1
    bot_state['last_scan'] = datetime.now().strftime('%H:%M:%S')
    socketio.emit('scan_update', {
        'scan_count': bot_state['scan_count'],
        'last_scan': bot_state['last_scan'],
        'asset': asset
    })

# ── WEBSOCKET CONNECTION ──────────────────────────────────────────────────────

async def connect_pocket_option():
    global SSID
    ssid = SSID
    if not ssid:
        print("No SSID — running demo mode")
        await demo_mode()
        return

    for endpoint in WS_ENDPOINTS:
        try:
            print(f"Trying: {endpoint}")
            bot_state['connection_status'] = f"Trying {endpoint}"
            socketio.emit('status_update', {
                'connected': False,
                'status': bot_state['connection_status']
            })

            async with websockets.connect(
                endpoint,
                extra_headers={
                    "Origin": "https://pocketoption.com",
                    "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) "
                                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                                  "Chrome/139.0.0.0 Mobile Safari/537.36"
                },
                ping_interval=20,
                ping_timeout=10,
                close_timeout=10
            ) as ws:
                print(f"Connected to {endpoint}")
                auth_sent = False
                current_asset = None
                asset_index = 0

                async def send_ping():
                    while True:
                        try:
                            await ws.send("3")
                            await asyncio.sleep(25)
                        except:
                            break

                async def cycle_assets():
                    nonlocal current_asset, asset_index
                    while bot_state['scanning']:
                        if asset_index >= len(KNOWN_OTC_ASSETS):
                            asset_index = 0
                        asset = KNOWN_OTC_ASSETS[asset_index]
                        current_asset = asset
                        asset_index += 1
                        msg = json.dumps([
                            "changeSymbol",
                            {"asset": asset, "period": 300}
                        ])
                        try:
                            await ws.send(f"42{msg}")
                            bot_state['assets_loaded'] = len(KNOWN_OTC_ASSETS)
                            socketio.emit('scan_update', {
                                'scan_count': bot_state['scan_count'],
                                'last_scan': bot_state['last_scan'],
                                'asset': asset
                            })
                        except:
                            break
                        await asyncio.sleep(8)

                async for message in ws:
                    if message == "2":
                        await ws.send("3")
                        continue

                    if message.startswith("0{") and not auth_sent:
                        await ws.send("40")
                        continue

                    if message == "40" and not auth_sent:
                        await ws.send(f"42{json.dumps(['auth', json.loads(ssid[2:] if ssid.startswith('42') else ssid)[1]])}" if ssid.startswith('42') else ssid)
                        auth_sent = True
                        asyncio.create_task(send_ping())
                        continue

                    if message.startswith("42"):
                        try:
                            data = json.loads(message[2:])
                            event = data[0] if isinstance(data, list) else None
                            payload = data[1] if isinstance(data, list) and len(data) > 1 else {}

                            if event in ["successauth", "0"]:
                                print("✅ Auth successful!")
                                bot_state['connected'] = True
                                bot_state['connection_status'] = "Connected"
                                socketio.emit('status_update', {
                                    'connected': True,
                                    'status': 'Connected to Pocket Option'
                                })
                                asyncio.create_task(cycle_assets())

                            elif event in ["candles", "history", "loadHistoryPeriod"]:
                                asset = payload.get("asset") or current_asset
                                candles_data = payload.get("candles") or payload.get("data", [])
                                if asset and candles_data:
                                    processed = []
                                    for c in candles_data:
                                        if isinstance(c, (list, tuple)) and len(c) >= 5:
                                            processed.append({
                                                'time': c[0],
                                                'open': float(c[1]),
                                                'close': float(c[2]),
                                                'high': float(c[3]),
                                                'low': float(c[4])
                                            })
                                        elif isinstance(c, dict):
                                            processed.append({
                                                'time': c.get('time', 0),
                                                'open': float(c.get('open', 0)),
                                                'close': float(c.get('close', 0)),
                                                'high': float(c.get('high', 0)),
                                                'low': float(c.get('low', 0))
                                            })
                                    if processed:
                                        candle_store[asset] = processed[-50:]
                                        run_analysis(asset)

                            elif event == "updateStream":
                                asset = payload.get("asset") or current_asset
                                if asset and payload:
                                    candle = {
                                        'time': payload.get('time', 0),
                                        'open': float(payload.get('o
