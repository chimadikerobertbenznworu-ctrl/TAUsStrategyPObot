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
    "connection_status": "Starting..."
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
    "EURUSD_otc", "GBPUSD_otc", "USDJPY_otc", "USDCHF_otc", "USDCAD_otc",
    "AUDUSD_otc", "NZDUSD_otc", "GBPJPY_otc", "EURJPY_otc", "EURGBP_otc",
    "AUDJPY_otc", "GBPAUD_otc", "GBPCAD_otc", "GBPCHF_otc", "EURCHF_otc",
    "EURAUD_otc", "EURCAD_otc", "AUDCAD_otc", "AUDCHF_otc", "AUDNZD_otc",
    "NZDCAD_otc", "NZDCHF_otc", "NZDJPY_otc", "CADJPY_otc", "CADCHF_otc",
    "CHFJPY_otc", "EURNZD_otc", "GBPNZD_otc", "USDRUB_otc", "USDTRY_otc",
    "BTCUSD_otc", "ETHUSD_otc", "LTCUSD_otc", "XRPUSD_otc", "SOLUSD_otc",
    "BNBUSD_otc", "DOGUSD_otc", "ADAUSD_otc", "DOTUSD_otc", "LNKUSD_otc",
    "#AAPL_otc", "#GOOG_otc", "#AMZN_otc", "#MSFT_otc", "#TSLA_otc",
    "#META_otc", "#NFLX_otc", "#NVDA_otc", "#AMD_otc", "#INTC_otc",
    "#COIN_otc", "#MARA_otc", "#PLTR_otc", "#GME_otc", "#BA_otc",
    "#FDX_otc", "#DIS_otc", "#MCD_otc", "#PFE_otc", "#SNAP_otc",
    "XAUUSD_otc", "XAGUSD_otc", "UKBRENT_otc", "USCRUDEOTC",
    "SP500_otc", "NASDAQ_otc", "DJ30_otc", "FTSE100_otc", "DAX30_otc",
]

candle_store = {}


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
            abs(candles[i]['high'] - candles[i - 1]['close']),
            abs(candles[i]['low'] - candles[i - 1]['close'])
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
    upper = middle + multiplier * atr
    lower = middle - multiplier * atr
    return upper, middle, lower


def calculate_stochastic(candles, k_period=14, d_period=3, smooth=3):
    if len(candles) < k_period + d_period + smooth:
        return None, None
    k_values = []
    for i in range(k_period - 1, len(candles)):
        window = candles[i - k_period + 1:i + 1]
        highest = max(c['high'] for c in window)
        lowest = min(c['low'] for c in window)
        close = candles[i]['close']
        if highest == lowest:
            k_values.append(50)
        else:
            k_values.append(100 * (close - lowest) / (highest - lowest))
    smoothed_k = []
    for i in range(smooth - 1, len(k_values)):
        smoothed_k.append(sum(k_values[i - smooth + 1:i + 1]) / smooth)
    if len(smoothed_k) < d_period:
        return None, None
    d_values = []
    for i in range(d_period - 1, len(smoothed_k)):
        d_values.append(sum(smoothed_k[i - d_period + 1:i + 1]) / d_period)
    return smoothed_k[-1], d_values[-1]


def detect_clean_trend(candles, lookback=6):
    if len(candles) < lookback:
        return None
    recent = candles[-lookback:]
    closes = [c['close'] for c in recent]
    bearish_count = sum(1 for c in recent if c['close'] < c['open'])
    bullish_count = sum(1 for c in recent if c['close'] > c['open'])
    if bearish_count >= 4:
        downward = sum(1 for i in range(1, len(closes)) if closes[i] < closes[i - 1])
        if downward >= 4:
            return "downtrend"
    if bullish_count >= 4:
        upward = sum(1 for i in range(1, len(closes)) if closes[i] > closes[i - 1])
        if upward >= 4:
            return "uptrend"
    return None


def analyze_main_strategy(asset, candles):
    if len(candles) < 30:
        return None
    analysis_candles = candles[:-1]
    trend = detect_clean_trend(analysis_candles, lookback=6)
    if not trend:
        return None
    upper_kc, middle_kc, lower_kc = calculate_keltner(analysis_candles)
    if upper_kc is None:
        return None
    stoch_k, stoch_d = calculate_stochastic(analysis_candles)
    if stoch_k is None:
        return None
    last_closed = analysis_candles[-1]
    prev_closed = analysis_candles[-2]

    if trend == "downtrend":
        stoch_rising = stoch_k > stoch_d and stoch_k < 40
        if not stoch_rising:
            return None
        approaching_lower = last_closed['low'] <= lower_kc * 1.002
        candle1_valid = (prev_closed['close'] > lower_kc and prev_closed['low'] <= lower_kc * 1.003)
        candle2_bullish = last_closed['close'] > last_closed['open']
        if not approaching_lower and not candle1_valid:
            if bot_state['setup_warnings']:
                return {"asset": asset, "direction": "BUY", "strategy": "main", "level": 1,
                        "level_name": "Setup Starting to Form - Watch Market",
                        "stoch_k": round(stoch_k, 2), "stoch_d": round(stoch_d, 2), "entry": False}
        elif approaching_lower and not candle1_valid:
            return {"asset": asset, "direction": "BUY", "strategy": "main", "level": 2,
                    "level_name": "Price Approaching Keltner Zone - Get Ready",
                    "stoch_k": round(stoch_k, 2), "stoch_d": round(stoch_d, 2), "entry": False}
        elif candle1_valid and not candle2_bullish:
            return {"asset": asset, "direction": "BUY", "strategy": "main", "level": 3,
                    "level_name": "Candle 1 Closed - Watch Confirmation Candle",
                    "stoch_k": round(stoch_k, 2), "stoch_d": round(stoch_d, 2), "entry": False}
        elif candle1_valid and candle2_bullish:
            return {"asset": asset, "direction": "BUY", "strategy": "main", "level": 5,
                    "level_name": "ENTRY SIGNAL",
                    "stoch_k": round(stoch_k, 2), "stoch_d": round(stoch_d, 2), "entry": True}

    elif trend == "uptrend":
        stoch_falling = stoch_k < stoch_d and stoch_k > 60
        if not stoch_falling:
            return None
        approaching_upper = last_closed['high'] >= upper_kc * 0.998
        candle1_valid = (prev_closed['close'] < upper_kc and prev_closed['high'] >= upper_kc * 0.997)
        candle2_bearish = last_closed['close'] < last_closed['open']
        if not approaching_upper and not candle1_valid:
            if bot_state['setup_warnings']:
                return {"asset": asset, "direction": "SELL", "strategy": "main", "level": 1,
                        "level_name": "Setup Starting to Form - Watch Market",
                        "stoch_k": round(stoch_k, 2), "stoch_d": round(stoch_d, 2), "entry": False}
        elif approaching_upper and not candle1_valid:
            return {"asset": asset, "direction": "SELL", "strategy": "main", "level": 2,
                    "level_name": "Price Approaching Keltner Zone - Get Ready",
                    "stoch_k": round(stoch_k, 2), "stoch_d": round(stoch_d, 2), "entry": False}
        elif candle1_valid and not candle2_bearish:
            return {"asset": asset, "direction": "SELL", "strategy": "main", "level": 3,
                    "level_name": "Candle 1 Closed - Watch Confirmation Candle",
                    "stoch_k": round(stoch_k, 2), "stoch_d": round(stoch_d, 2), "entry": False}
        elif candle1_valid and candle2_bearish:
            return {"asset": asset, "direction": "SELL", "strategy": "main", "level": 5,
                    "level_name": "ENTRY SIGNAL",
                    "stoch_k": round(stoch_k, 2), "stoch_d": round(stoch_d, 2), "entry": True}
    return None


def analyze_pattern_strategy(asset, candles):
    if len(candles) < 10:
        return None
    trend = detect_clean_trend(candles[:-3], lookback=6)
    if not trend:
        return None
    c1 = candles[-3]
    c2 = candles[-2]
    c3 = candles[-1]
    c1_body = abs(c1['close'] - c1['open'])
    c2_body = abs(c2['close'] - c2['open'])
    if c1_body == 0:
        return None
    ratio = c2_body / c1_body

    if trend == "uptrend":
        if not (c1['close'] > c1['open'] and c2['close'] < c2['open']):
            return None
        if ratio >= 0.5:
            return {"asset": asset, "direction": "SELL", "strategy": "pattern",
                    "pattern_type": "Type 1 - Full/Partial Match", "level": 3,
                    "level_name": "Pattern Confirmed - Enter SELL on Next Candle",
                    "ratio": round(ratio * 100), "entry": True}
        elif 0.05 < ratio < 0.5:
            if c3['close'] < c3['open']:
                return {"asset": asset, "direction": "SELL", "strategy": "pattern",
                        "pattern_type": "Type 2 - Hammer", "level": 5,
                        "level_name": "Hammer Confirmed - Enter SELL on Next Candle",
                        "ratio": round(ratio * 100), "entry": True}
            else:
                return {"asset": asset, "direction": "SELL", "strategy": "pattern",
                        "pattern_type": "Type 2 - Hammer", "level": 3,
                        "level_name": "Hammer Candle Confirmed - Watch Next Candle",
                        "ratio": round(ratio * 100), "entry": False}

    elif trend == "downtrend":
        if not (c1['close'] < c1['open'] and c2['close'] > c2['open']):
            return None
        if ratio >= 0.5:
            return {"asset": asset, "direction": "BUY", "strategy": "pattern",
                    "pattern_type": "Type 1 - Full/Partial Match", "level": 3,
                    "level_name": "Pattern Confirmed - Enter BUY on Next Candle",
                    "ratio": round(ratio * 100), "entry": True}
        elif 0.05 < ratio < 0.5:
            if c3['close'] > c3['open']:
                return {"asset": asset, "direction": "BUY", "strategy": "pattern",
                        "pattern_type": "Type 2 - Hammer", "level": 5,
                        "level_name": "Hammer Confirmed - Enter BUY on Next Candle",
                        "ratio": round(ratio * 100), "entry": True}
            else:
                return {"asset": asset, "direction": "BUY", "strategy": "pattern",
                        "pattern_type": "Type 2 - Hammer", "level": 3,
                        "level_name": "Hammer Candle Confirmed - Watch Next Candle",
                        "ratio": round(ratio * 100), "entry": False}
    return None


def send_telegram(message):
    if not bot_state["telegram_alerts"] or not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=5
        )
    except Exception as e:
        print(f"Telegram error: {e}")


def format_signal_message(signal):
    d = "🟢" if signal['direction'] == "BUY" else "🔴"
    e = "✅" if signal.get('entry') else "⚠️"
    s = "Keltner Channel" if signal['strategy'] == "main" else "Pattern Recognition"
    msg = f"{e} <b>{signal['level_name']}</b>\n\n"
    msg += f"{d} <b>{signal['direction']}</b> — {signal['asset']}\n"
    msg += f"📊 Strategy: {s}\n"
    if signal['strategy'] == "pattern":
        msg += f"🕯 Pattern: {signal.get('pattern_type', '')}\n"
        msg += f"📏 Size: {signal.get('ratio', '')}%\n"
    else:
        msg += f"📈 Stoch K: {signal.get('stoch_k', '')} | D: {signal.get('stoch_d', '')}\n"
    msg += f"⏰ {signal.get('time', '')} | M5\n"
    if signal.get('entry'):
        msg += f"\n<b>➡️ Enter {signal['direction']} on next candle</b>"
    return msg


def process_signal(signal):
    if not signal:
        return
    signal['time'] = datetime.now().strftime('%H:%M:%S')
    signal['id'] = f"{signal['asset']}_{int(time.time())}"
    bot_state['signals'].insert(0, signal)
    bot_state['signals'] = bot_state['signals'][:50]
    socketio.emit('new_signal', signal)
    if signal.get('entry') or signal.get('level', 0) >= 3:
        send_telegram(format_signal_message(signal))
    print(f"[{signal['time']}] {signal['direction']} {signal['asset']} — {signal['level_name']}")


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


def parse_candle(c):
    try:
        if isinstance(c, (list, tuple)) and len(c) >= 5:
            return {'time': c[0], 'open': float(c[1]), 'close': float(c[2]),
                    'high': float(c[3]), 'low': float(c[4])}
        elif isinstance(c, dict):
            return {'time': c.get('time', 0), 'open': float(c.get('open', 0)),
                    'close': float(c.get('close', 0)), 'high': float(c.get('high', 0)),
                    'low': float(c.get('low', 0))}
    except Exception:
        pass
    return None


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
            bot_state['connection_status'] = "Connecting..."
            socketio.emit('status_update', {'connected': False, 'status': 'Connecting...'})

            async with websockets.connect(
                endpoint,
                extra_headers={
                    "Origin": "https://pocketoption.com",
                    "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36"
                },
                ping_interval=20,
                ping_timeout=15,
                close_timeout=10
            ) as ws:
                auth_sent = False
                current_asset = None
                asset_index = 0

                async def keep_alive():
                    while True:
                        try:
                            await asyncio.sleep(25)
                            await ws.send("3")
                        except Exception:
                            break

                async def cycle_assets():
                    nonlocal current_asset, asset_index
                    await asyncio.sleep(2)
                    while True:
                        if not bot_state['scanning']:
                            await asyncio.sleep(5)
                            continue
                        if asset_index >= len(KNOWN_OTC_ASSETS):
                            asset_index = 0
                        asset = KNOWN_OTC_ASSETS[asset_index]
                        current_asset = asset
                        asset_index += 1
                        try:
                            msg = json.dumps(["changeSymbol", {"asset": asset, "period": 300}])
                            await ws.send(f"42{msg}")
                            socketio.emit('scan_update', {
                                'scan_count': bot_state['scan_count'],
                                'last_scan': bot_state['last_scan'],
                                'asset': asset
                            })
                        except Exception:
                            break
                        await asyncio.sleep(8)

                async for raw in ws:
                    if raw == "2":
                        await ws.send("3")
                        continue
                    if raw.startswith("0{") and not auth_sent:
                        await ws.send("40")
                        continue
                    if raw == "40" and not auth_sent:
                        try:
                            await ws.send(ssid if ssid.startswith("42") else f"42{ssid}")
                            auth_sent = True
                            asyncio.create_task(keep_alive())
                        except Exception as e:
                            print(f"Auth error: {e}")
                        continue
                    if raw.startswith("42"):
                        try:
                            data = json.loads(raw[2:])
                            if not isinstance(data, list) or len(data) < 1:
                                continue
                            event = data[0]
                            payload = data[1] if len(data) > 1 else {}

                            if event in ["successauth", "0", "authorized"]:
                                bot_state['connected'] = True
                                bot_state['connection_status'] = "Connected"
                                bot_state['assets_loaded'] = len(KNOWN_OTC_ASSETS)
                                socketio.emit('status_update', {
                                    'connected': True,
                                    'status': 'Connected to Pocket Option LIVE'
                                })
                                print("CONNECTED TO POCKET OPTION")
                                asyncio.create_task(cycle_assets())

                            elif event in ["candles", "history", "loadHistoryPeriod", "successloadhistory"]:
                                asset = payload.get("asset") or current_asset
                                raw_candles = payload.get("candles") or payload.get("data") or []
                                if asset and raw_candles:
                                    processed = [parse_candle(c) for c in raw_candles]
                                    processed = [c for c in processed if c]
                                    if processed:
                                        candle_store[asset] = processed[-50:]
                                        run_analysis(asset)

                            elif event in ["updateStream", "stream", "tick"]:
                                asset = payload.get("asset") or current_asset
                                if asset and payload:
                                    candle = parse_candle(payload)
                                    if candle:
                                        if asset not in candle_store:
                                            candle_store[asset] = []
                                        candle_store[asset].append(candle)
                                        candle_store[asset] = candle_store[asset][-50:]
                                        run_analysis(asset)

                        except Exception as e:
                            print(f"Parse error: {e}")
                            continue

        except Exception as e:
            print(f"Endpoint failed {endpoint}: {e}")
            await asyncio.sleep(3)
            continue

    print("All endpoints failed — demo mode")
    await demo_mode()


async def demo_mode():
    import random
    bot_state['connected'] = True
    bot_state['connection_status'] = "Demo Mode"
    bot_state['assets_loaded'] = len(KNOWN_OTC_ASSETS)
    socketio.emit('status_update', {
        'connected': True, 'demo_mode': True,
        'status': 'Demo Mode — Add valid SSID to connect live'
    })
    level_names = {
        1: "Setup Starting to Form - Watch Market",
        2: "Price Approaching Keltner Zone - Get Ready",
        3: "Candle 1 Closed - Watch Confirmation Candle",
        5: "ENTRY SIGNAL"
    }
    while bot_state['scanning']:
        asset = random.choice(KNOWN_OTC_ASSETS)
        level = random.choice([1, 2, 3, 5])
        strategy = random.choice(["main", "pattern"])
        direction = random.choice(["BUY", "SELL"])
        signal = {
            "asset": asset, "direction": direction,
            "strategy": strategy, "level": level,
            "level_name": level_names[level],
            "entry": level == 5, "demo": True
        }
        if strategy == "pattern" and level >= 3:
            signal["pattern_type"] = random.choice(["Type 1 - Full/Partial Match", "Type 2 - Hammer"])
            signal["ratio"] = random.randint(50, 110)
        process_signal(signal)
        await asyncio.sleep(10)


def run_bot():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    while True:
        try:
            loop.run_until_complete(connect_pocket_option())
        except Exception as e:
            print(f"Bot error: {e}")
        time.sleep(5)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/state')
def get_state():
    return jsonify({
        "scanning": bot_state['scanning'],
        "telegram_alerts": bot_state['telegram_alerts'],
        "sound_alerts": bot_state['sound_alerts'],
        "main_strategy": bot_state['main_strategy'],
        "pattern_strategy": bot_state['pattern_strategy'],
        "setup_warnings": bot_state['setup_warnings'],
        "connected": bot_state['connected'],
        "scan_count": bot_state['scan_count'],
        "last_scan": bot_state['last_scan'],
        "assets_loaded": bot_state['assets_loaded'],
        "signals": bot_state['signals'][:20],
        "connection_status": bot_state['connection_status']
    })


@app.route('/api/toggle', methods=['POST'])
def toggle():
    key = request.json.get('key')
    if key in bot_state:
        bot_state[key] = not bot_state[key]
        socketio.emit('state_update', {key: bot_state[key]})
        return jsonify({"success": True, "value": bot_state[key]})
    return jsonify({"success": False})


@app.route('/api/update_ssid', methods=['POST'])
def update_ssid():
    global SSID
    new_ssid = request.json.get('ssid', '').strip()
    if not new_ssid:
        return jsonify({"success": False, "message": "Empty SSID"})
    SSID = new_ssid
    bot_state['connected'] = False
    bot_state['connection_status'] = "Reconnecting with new SSID..."
    socketio.emit('status_update', {'connected': False, 'status': 'Reconnecting...'})
    t = threading.Thread(target=run_bot, daemon=True)
    t.start()
    return jsonify({"success": True, "message": "SSID updated. Reconnecting..."})


@app.route('/ping')
def ping():
    return "OK", 200


@socketio.on('connect')
def on_connect():
    emit('state_update', bot_state)
    emit('status_update', {
        'connected': bot_state['connected'],
        'status': bot_state['connection_status']
    })


if __name__ == '__main__':
    t = threading.Thread(target=run_bot, daemon=True)
    t.start()
    port = int(os.environ.get('PORT', 5000))
    socketio.run(app, host='0.0.0.0', port=port, debug=False)
