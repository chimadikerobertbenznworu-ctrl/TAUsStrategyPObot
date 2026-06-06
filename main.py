import asyncio
import os
import time
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
    "assets_loaded": 0
}

SSID = os.environ.get("PO_SSID", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
IS_DEMO = os.environ.get("IS_DEMO", "1") == "1"
MIN_PAYOUT = int(os.environ.get("MIN_PAYOUT", "80"))

# Full known OTC asset list — bot will also fetch live list from API
KNOWN_OTC_ASSETS = [
    # Forex OTC
    "EURUSD_otc","GBPUSD_otc","USDJPY_otc","USDCHF_otc","USDCAD_otc",
    "AUDUSD_otc","NZDUSD_otc","GBPJPY_otc","EURJPY_otc","EURGBP_otc",
    "AUDJPY_otc","GBPAUD_otc","GBPCAD_otc","GBPCHF_otc","EURCHF_otc",
    "EURAUD_otc","EURCAD_otc","AUDCAD_otc","AUDCHF_otc","AUDNZD_otc",
    "NZDCAD_otc","NZDCHF_otc","NZDJPY_otc","CADJPY_otc","CADCHF_otc",
    "CHFJPY_otc","EURNZD_otc","GBPNZD_otc","USDRUB_otc","USDTRY_otc",
    "USDBRL_otc","USDMXN_otc","USDZAR_otc","USDSGD_otc","USDHKD_otc",
    "USDNOK_otc","USDSEK_otc","USDDKK_otc","USDPLN_otc","USDHUF_otc",
    "USDCZK_otc","USDILS_otc","USDTHB_otc","USDINR_otc","USDMYR_otc",
    "USDPHP_otc","USDIDR_otc","USDNGN_otc","USDKES_otc","USDZAR_otc",
    "EURPLN_otc","EURTRY_otc","EURHUF_otc","EURCZK_otc",
    # Crypto OTC
    "BTCUSD_otc","ETHUSD_otc","LTCUSD_otc","XRPUSD_otc","BCHUSD_otc",
    "EOSUSD_otc","XLMUSD_otc","ADAUSD_otc","DOTUSD_otc","SOLUSD_otc",
    "DOGUSD_otc","LNKUSD_otc","BNBUSD_otc","MATUSD_otc","AVXUSD_otc",
    "SHIBUSD_otc","UNIUSD_otc","ATOMUSD_otc","ALGOUSD_otc","FTMUSD_otc",
    # Stocks OTC
    "#AAPL_otc","#GOOG_otc","#AMZN_otc","#MSFT_otc","#TSLA_otc",
    "#META_otc","#NFLX_otc","#NVDA_otc","#AMD_otc","#INTC_otc",
    "#BABA_otc","#TWTR_otc","#SNAP_otc","#UBER_otc","#LYFT_otc",
    "#COIN_otc","#MARA_otc","#PLTR_otc","#GME_otc","#AMC_otc",
    "#BA_otc","#FDX_otc","#DIS_otc","#MCD_otc","#PFE_otc",
    "#JNJ_otc","#V_otc","#MA_otc","#JPM_otc","#GS_otc",
    "#PYPL_otc","#SQ_otc","#SHOP_otc","#ZOOM_otc","#TWLO_otc",
    # Commodities OTC
    "XAUUSD_otc","XAGUSD_otc","XPTUSD_otc",
    "UKBRENT_otc","USCRUDEOTC","NATGAS_otc","WHEAT_otc",
    # Indices OTC
    "SP500_otc","NASDAQ_otc","DJ30_otc","FTSE100_otc",
    "DAX30_otc","CAC40_otc","NIKKEI_otc","ASX200_otc",
    "VIX_otc","RUT2000_otc"
]

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
        k_values.append(50 if highest == lowest else 100 * (close - lowest) / (highest - lowest))
    smoothed_k = [sum(k_values[i-smooth+1:i+1])/smooth for i in range(smooth-1, len(k_values))]
    if len(smoothed_k) < d_period:
        return None, None
    d_values = [sum(smoothed_k[i-d_period+1:i+1])/d_period for i in range(d_period-1, len(smoothed_k))]
    return smoothed_k[-1], d_values[-1]

def detect_clean_trend(candles, lookback=6):
    if len(candles) < lookback:
        return None
    recent = candles[-lookback:]
    closes = [c['close'] for c in recent]
    bearish = sum(1 for c in recent if c['close'] < c['open'])
    bullish = sum(1 for c in recent if c['close'] > c['open'])
    if bearish >= 4:
        if sum(1 for i in range(1,len(closes)) if closes[i] < closes[i-1]) >= 4:
            return "downtrend"
    if bullish >= 4:
        if sum(1 for i in range(1,len(closes)) if closes[i] > closes[i-1]) >= 4:
            return "uptrend"
    return None

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
        c1_valid = prev['close'] > lower_kc and prev['low'] <= lower_kc * 1.002
        c2_bull = last['close'] > last['open']
        if stoch_ok and approaching and not c1_valid:
            return {"asset":asset,"direction":"BUY","strategy":"main","level":2,"level_name":"Price Approaching Keltner Zone","stoch_k":round(stoch_k,2),"stoch_d":round(stoch_d,2)}
        elif stoch_ok and c1_valid and not c2_bull:
            return {"asset":asset,"direction":"BUY","strategy":"main","level":3,"level_name":"Candle 1 Closed — Watch Confirmation Candle","stoch_k":round(stoch_k,2),"stoch_d":round(stoch_d,2)}
        elif stoch_ok and c1_valid and c2_bull:
            return {"asset":asset,"direction":"BUY","strategy":"main","level":5,"level_name":"ENTRY SIGNAL","stoch_k":round(stoch_k,2),"stoch_d":round(stoch_d,2),"entry":True}
    elif trend == "uptrend":
        stoch_ok = stoch_k < stoch_d and stoch_k > 60
        approaching = last['high'] >= upper_kc * 0.999
        c1_valid = prev['close'] < upper_kc and prev['high'] >= upper_kc * 0.998
        c2_bear = last['close'] < last['open']
        if stoch_ok and approaching and not c1_valid:
            return {"asset":asset,"direction":"SELL","strategy":"main","level":2,"level_name":"Price Approaching Keltner Zone","stoch_k":round(stoch_k,2),"stoch_d":round(stoch_d,2)}
        elif stoch_ok and c1_valid and not c2_bear:
            return {"asset":asset,"direction":"SELL","strategy":"main","level":3,"level_name":"Candle 1 Closed — Watch Confirmation Candle","stoch_k":round(stoch_k,2),"stoch_d":round(stoch_d,2)}
        elif stoch_ok and c1_valid and c2_bear:
            return {"asset":asset,"direction":"SELL","strategy":"main","level":5,"level_name":"ENTRY SIGNAL","stoch_k":round(stoch_k,2),"stoch_d":round(stoch_d,2),"entry":True}
    return None

def analyze_pattern_strategy(asset, candles):
    if len(candles) < 10:
        return None
    trend = detect_clean_trend(candles[:-2])
    if not trend:
        return None
    c1,c2,c3 = candles[-3],candles[-2],candles[-1]
    c1_body = abs(c1['close']-c1['open'])
    c2_body = abs(c2['close']-c2['open'])
    if c1_body == 0:
        return None
    ratio = c2_body / c1_body
    if trend == "uptrend" and c1['close']>c1['open'] and c2['close']<c2['open']:
        if ratio >= 0.5:
            return {"asset":asset,"direction":"SELL","strategy":"pattern","pattern_type":"Type 1 (Full/Partial Match)","level":3,"level_name":"Pattern Confirmed — Enter SELL on Next Candle","ratio":round(ratio*100),"entry":True}
        elif 0.05 < ratio < 0.5:
            if c3['close'] < c3['open']:
                return {"asset":asset,"direction":"SELL","strategy":"pattern","pattern_type":"Type 2 (Hammer)","level":5,"level_name":"Hammer Confirmed — Enter SELL on Next Candle","ratio":round(ratio*100),"entry":True}
            else:
                return {"asset":asset,"direction":"SELL","strategy":"pattern","pattern_type":"Type 2 (Hammer)","level":3,"level_name":"Hammer Candle Confirmed — Watch Next Candle","ratio":round(ratio*100),"entry":False}
    elif trend == "downtrend" and c1['close']<c1['open'] and c2['close']>c2['open']:
        if ratio >= 0.5:
            return {"asset":asset,"direction":"BUY","strategy":"pattern","pattern_type":"Type 1 (Full/Partial Match)","level":3,"level_name":"Pattern Confirmed — Enter BUY on Next Candle","ratio":round(ratio*100),"entry":True}
        elif 0.05 < ratio < 0.5:
            if c3['close'] > c3['open']:
                return {"asset":asset,"direction":"BUY","strategy":"pattern","pattern_type":"Type 2 (Hammer)","level":5,"level_name":"Hammer Confirmed — Enter BUY on Next Candle","ratio":round(ratio*100),"entry":True}
            else:
                return {"asset":asset,"direction":"BUY","strategy":"pattern","pattern_type":"Type 2 (Hammer)","level":3,"level_name":"Hammer Candle Confirmed — Watch Next Candle","ratio":round(ratio*100),"entry":False}
    return None

def send_telegram(message):
    if not bot_state["telegram_alerts"] or not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id":TELEGRAM_CHAT_ID,"text":message,"parse_mode":"HTML"}, timeout=5)
    except Exception as e:
        print(f"Telegram error: {e}")

def format_signal(signal):
    d = "🟢" if signal['direction']=="BUY" else "🔴"
    e = "✅" if signal.get('entry') else "⚠️"
    s = "Keltner Channel" if signal['strategy']=="main" else "Pattern Recognition"
    msg = f"{e} <b>{signal['level_name']}</b>\n\n{d} <b>{signal['direction']}</b> — {signal['asset']}\n"
    msg += f"📊 Strategy: {s}\n"
    if signal['strategy']=="pattern":
        msg += f"🕯 Pattern: {signal.get('pattern_type','')}\n📏 Size: {signal.get('ratio','')}%\n"
    msg += f"⏰ {datetime.now().strftime('%H:%M:%S')} | M5\n"
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
    if signal.get('entry') or signal.get('level',0) >= 3:
        send_telegram(format_signal(signal))
    print(f"[{signal['time']}] {signal['direction']} {signal['asset']} — {signal['level_name']}")

async def scan_assets():
    try:
        from pocket_option import PocketOption
        api = PocketOption(SSID, IS_DEMO)
        await api.connect()
        bot_state['connected'] = True
        socketio.emit('status_update', {'connected': True})
        print("✅ Connected to Pocket Option")
        # Try to get full asset list from API
        try:
            all_assets = await api.get_available_assets()
            otc_assets = [a for a in all_assets if '_otc' in a.lower() or 'otc' in a.lower()]
            print(f"✅ Loaded {len(otc_assets)} OTC assets from Pocket Option")
        except:
            otc_assets = KNOWN_OTC_ASSETS
            print(f"Using built-in list of {len(otc_assets)} OTC assets")
        bot_state['assets_loaded'] = len(otc_assets)
        socketio.emit('assets_loaded', {'count': len(otc_assets)})
        while bot_state['scanning']:
            for asset in otc_assets:
                if not bot_state['scanning']:
                    break
                try:
                    candles = await api.get_candles(asset, 300, 50)
                    if not candles or len(candles) < 30:
                        continue
                    if bot_state['main_strategy']:
                        process_signal(analyze_main_strategy(asset, candles))
                    if bot_state['pattern_strategy']:
                        process_signal(analyze_pattern_strategy(asset, candles))
                    bot_state['scan_count'] += 1
                    bot_state['last_scan'] = datetime.now().strftime('%H:%M:%S')
                    socketio.emit('scan_update', {'scan_count':bot_state['scan_count'],'last_scan':bot_state['last_scan'],'asset':asset})
                    await asyncio.sleep(0.5)
                except Exception as e:
                    print(f"Error {asset}: {e}")
            await asyncio.sleep(5)
    except ImportError:
        print("pocket_option not installed — running demo mode")
        await demo_mode()
    except Exception as e:
        print(f"Connection error: {e}")
        bot_state['connected'] = False
        socketio.emit('status_update', {'connected': False})

async def demo_mode():
    import random
    bot_state['connected'] = True
    bot_state['assets_loaded'] = len(KNOWN_OTC_ASSETS)
    socketio.emit('status_update', {'connected': True, 'demo_mode': True})
    socketio.emit('assets_loaded', {'count': len(KNOWN_OTC_ASSETS)})
    names = {2:"Price Approaching Keltner Zone",3:"Candle 1 Closed — Watch Confirmation",5:"ENTRY SIGNAL"}
    while bot_state['scanning']:
        asset = random.choice(KNOWN_OTC_ASSETS)
        level = random.choice([2,3,5])
        signal = {"asset":asset,"direction":random.choice(["BUY","SELL"]),"strategy":random.choice(["main","pattern"]),"level":level,"level_name":names[level],"entry":level==5,"demo":True}
        process_signal(signal)
        await asyncio.sleep(12)

def run_bot():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(scan_assets())

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/state')
def get_state():
    return jsonify({k: bot_state[k] for k in ["scanning","telegram_alerts","sound_alerts","main_strategy","pattern_strategy","setup_warnings","connected","scan_count","last_scan","assets_loaded","signals"]})

@app.route('/api/toggle', methods=['POST'])
def toggle():
    key = request.json.get('key')
    if key in bot_state:
        bot_state[key] = not bot_state[key]
        socketio.emit('state_update', {key: bot_state[key]})
        return jsonify({"success":True,"value":bot_state[key]})
    return jsonify({"success":False})

@app.route('/ping')
def ping():
    return "OK", 200

@socketio.on('connect')
def on_connect():
    emit('state_update', bot_state)

if __name__ == '__main__':
    t = threading.Thread(target=run_bot, daemon=True)
    t.start()
    port = int(os.environ.get('PORT', 5000))
    socketio.run(app, host='0.0.0.0', port=port, debug=False)
