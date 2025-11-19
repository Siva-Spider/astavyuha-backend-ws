# tasks/trading_tasks.py
from backend.celery_app import celery_app
import backend.logger_util as logger_util
import json, datetime, time, gc
from zoneinfo import ZoneInfo

# ---- import your broker and helper modules ----
from backend import Upstox as us
from backend import Zerodha as zr
from backend import AngelOne as ar
from backend import Groww as gr
from backend import Fivepaisa as fp
from backend import Next_Now_intervals as nni
from backend import combinding_dataframes as cdf
from backend import indicators as ind
from time import sleep as gsleep
import redis
import os
import ssl

# ---- Celery setup ----
REDIS_URL = os.getenv("REDIS_URL", "").strip()
if not REDIS_URL:
    print("⚠️ REDIS_URL not found, defaulting to localhost Redis (for dev).")
    REDIS_URL = "redis://localhost:6379"

print(f"🚀 Initializing Celery with broker: {REDIS_URL}")

# LOCAL VERSION (your original code)
r = redis.StrictRedis.from_url(REDIS_URL, decode_responses=True)

r = redis.StrictRedis.from_url(REDIS_URL, decode_responses=True)

stock_map = {
    "RELIANCE INDUSTRIES LTD": "RELIANCE",
    "HDFC BANK LTD": "HDFCBANK",
    "ICICI BANK LTD.": "ICICIBANK",
    "INFOSYS LIMITED": "INFY",
    "TATA CONSULTANCY SERV LT": "TCS",
    "STATE BANK OF INDIA": "SBIN",
    "AXIS BANK LTD": "AXISBANK",
    "KOTAK MAHINDRA BANK LTD": "KOTAKBANK",
    "ITC LTD": "ITC",
    "LARSEN & TOUBRO LTD.": "LT",
    "BAJAJ FINANCE LIMITED": "BAJFINANCE",
    "HINDUSTAN UNILEVER LTD": "HINDUNILVR",
    "SUN PHARMACEUTICAL IND L": "SUNPHARMA",
    "MARUTI SUZUKI INDIA LTD": "MARUTI",
    "NTPC LTD": "NTPC",
    "HCL TECHNOLOGIES LTD": "HCLTECH",
    "ULTRATECH CEMENT LIMITED": "ULTRACEMCO",
    "TATA MOTORS LIMITED": "TATAMOTORS",
    "TITAN COMPANY LIMITED": "TITAN",
    "BHARAT ELECTRONICS LTD": "BEL",
    "POWER GRID CORP. LTD": "POWERGRID",
    "TATA STEEL LIMITED": "TATASTEEL",
    "TRENT LTD": "TRENT",
    "ASIAN PAINTS LIMITED": "ASIANPAINT",
    "JIO FIN SERVICES LTD": "JIOFIN",
    "BAJAJ FINSERV LTD": "BAJAJFINSV",
    "GRASIM INDUSTRIES LTD": "GRASIM",
    "ADANI PORT & SEZ LTD": "ADANIPORTS",
    "JSW STEEL LIMITED": "JSWSTEEL",
    "HINDALCO INDUSTRIES LTD": "HINDALCO",
    "OIL AND NATURAL GAS CORP": "ONGC",
    "TECH MAHINDRA LIMITED": "TECHM",
    "BAJAJ AUTO LIMITED": "BAJAJ-AUTO",
    "SHRIRAM FINANCE LIMITED": "SHRIRAMFIN",
    "CIPLA LTD": "CIPLA",
    "COAL INDIA LTD": "COALINDIA",
    "SBI LIFE INSURANCE CO LTD": "SBILIFE",
    "HDFC LIFE INS CO LTD": "HDFCLIFE",
    "NESTLE INDIA LIMITED": "NESTLEIND",
    "DR. REDDY S LABORATORIES": "DRREDDY",
    "APOLLO HOSPITALS ENTER. L": "APOLLOHOSP",
    "EICHER MOTORS LTD": "EICHERMOT",
    "WIPRO LTD": "WIPRO",
    "TATA CONSUMER PRODUCT LTD": "TATACONSUM",
    "ADANI ENTERPRISES LIMITED": "ADANIENT",
    "HERO MOTOCORP LIMITED": "HEROMOTOCO",
    "INDUSIND BANK LIMITED": "INDUSINDBK",
    "Nifty 50": "NIFTY",
    "Nifty Bank": "BANKNIFTY",
    "Nifty Fin Service": "FINNIFTY",
    "NIFTY MID SELECT": "MIDCPNIFTY",
}

# keep same broker maps
broker_map = {
    "u": "Upstox",
    "z": "Zerodha",
    "a": "AngelOne",
    "g": "Groww",
    "5": "5paisa"
}

celery_app.conf.task_track_started = True
celery_app.conf.worker_concurrency = 4

# ---- shared global maps ----
active_trades = {}
broker_map = {"u": "upstox", "z": "zerodha", "a": "angelone", "f": "5paisa", "g": "groww"}
reverse_stock_map = {}

@celery_app.task(bind=True)
def start_trading_loop(self):
    logger_util.push_log("🚀 Trading engine started inside Celery worker.")
    try:
        with open("trading_config.json", "r") as f:
            config = json.load(f)
        trading_parameters = config.get("tradingParameters", [])
        selected_brokers = config.get("selectedBrokers", [])
    except Exception as e:
        logger_util.push_log(f"❌ Could not read trading_config.json: {e}", "error")
        return

    try:
        run_trading_logic_for_all(trading_parameters, selected_brokers)
    except Exception as e:
        logger_util.push_log(f"💥 Trading loop crashed: {e}", "error")
        return

    logger_util.push_log("✅ Trading engine task finished successfully.")


def run_trading_logic_for_all(trading_parameters, selected_brokers):
    logger_util.push_log("✅ Trading loop started for all selected stocks")
    logger_util.push_log("⏳ Starting trading cycle setup...")

    for stock in trading_parameters:
        active_trades[stock['symbol_value']] = True

    # STEP 1: Fetch instrument keys
    for stock in trading_parameters:
        if not active_trades.get(stock['symbol_value']):
            continue

        broker_key = stock.get('broker')
        broker_name = broker_map.get(broker_key, "unknown")
        symbol = stock.get('symbol_value')
        name = stock.get('symbol_key')
        strategy = stock.get('strategy')
        company = stock.get("symbol_key", symbol)
        interval = stock.get('interval')
        exchange_type = stock.get('type')

        logger_util.push_log(
            f"🔑 Fetching instrument key for company : {company}, Name : {name} symbol :{symbol} via Broker : {broker_name}..."
        )

        instrument_key = None
        try:
            if exchange_type == "EQUITY":
                if broker_name == "upstox":
                    instrument_key = us.upstox_equity_instrument_key(company)
                elif broker_name == "zerodha":
                    broker_info = next(
                        (b for b in selected_brokers if b['name'] == broker_key), None
                    )
                    if broker_info:
                        api_key = broker_info['credentials'].get("api_key")
                        access_token = broker_info['credentials'].get("access_token")
                        instrument_key = zr.zerodha_instruments_token(
                            api_key, access_token, symbol
                        )
                elif broker_name == "angelone":
                    instrument_key = ar.angelone_get_token_by_name(symbol)
                elif broker_name == "5paisa":
                    instrument_key = fp.fivepaisa_scripcode_fetch(symbol)

            elif exchange_type == "COMMODITY" and broker_name == "upstox":
                matched = us.upstox_commodity_instrument_key(name, symbol)
                instrument_key = matched['instrument_key'].iloc[0]

            if instrument_key:
                stock['instrument_key'] = instrument_key
                logger_util.push_log(f"✅ Found instrument key {instrument_key} for {symbol}")
            else:
                logger_util.push_log(f"⚠️ No instrument key found for {symbol}, skipping.", "warning")
                active_trades[symbol] = False

        except Exception as e:
            logger_util.push_log(f"❌ Error fetching instrument key for {symbol}: {e}", "error")
            active_trades[symbol] = False

    # STEP 2: Interval setup
    interval = trading_parameters[0].get("interval", "1minute")
    now_interval, next_interval = nni.round_to_next_interval(interval)
    logger_util.push_log(f"🕓 Present Interval Start: {now_interval}, Next Interval: {next_interval}")

    r = redis.StrictRedis(host="localhost", port=6379, db=5, decode_responses=True)

    symbols = [s["symbol_value"] for s in trading_parameters if s.get("symbol_value")]
    if not symbols:
        logger_util.push_log("⚠️ No valid symbols to start trading. Exiting.", "warning")
        return

    r.delete("active_trades")
    r.sadd("active_trades", *symbols)
    logger_util.push_log(f"🟢 Active trades initialized in Redis: {', '.join(symbols)}")

    time.sleep(0.5)

    # STEP 3: Trading loop
    while True:
        active_symbols = set(r.smembers("active_trades"))

        trading_parameters = [
            s for s in trading_parameters if s["symbol_value"] in active_symbols
        ]

        if not trading_parameters:
            logger_util.push_log("🏁 All trades stopped — exiting trading loop.")
            break

        now = datetime.datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")

        if now >= next_interval:
            now_interval, next_interval = nni.round_to_next_interval(interval)
            logger_util.push_log(f"⏱ New interval reached: {now_interval}")

            # Fetch candles
            for stock in trading_parameters:
                symbol = stock.get('symbol_value')
                broker_key = stock.get('broker')
                broker_name = broker_map.get(broker_key)
                company = stock.get('symbol_key')
                interval = stock.get('interval')
                instrument_key = stock.get('instrument_key')
                strategy = stock.get('strategy')
                exchange_type = stock.get('type')
                tick_size = stock.get('tick_size')

                logger_util.push_log(f"🕯 Fetching candles for {symbol}-{company} from {broker_name}")

                combined_df = None
                try:
                    if broker_name == "upstox":
                        access_token = next(
                            (b['credentials']['access_token']
                             for b in selected_brokers if b['name'] == broker_key),
                            None
                        )
                        if access_token:
                            hdf = us.upstox_fetch_historical_data_with_retry(
                                access_token, instrument_key, interval
                            )
                            idf = us.upstox_fetch_intraday_data(
                                access_token, instrument_key, interval
                            )
                            if hdf is not None and idf is not None:
                                combined_df = cdf.combinding_dataframes(hdf, idf)

                    elif broker_name == "zerodha":
                        broker_info = next(
                            (b for b in selected_brokers if b['name'] == broker_key), None
                        )
                        if broker_info:
                            kite = zr.kite_connect_from_credentials(
                                broker_info['credentials']
                            )
                            hdf = zr.zerodha_historical_data(kite, instrument_key, interval)
                            idf = zr.zerodha_intraday_data(kite, instrument_key, interval)
                            if hdf is not None and idf is not None:
                                combined_df = cdf.combinding_dataframes(hdf, idf)

                    elif broker_name == "angelone":
                        broker_info = next(
                            (b for b in selected_brokers if b['name'] == broker_key), None
                        )
                        if broker_info:
                            api_key = broker_info['credentials'].get("api_key")
                            user_id = broker_info['credentials'].get("user_id")
                            pin = broker_info['credentials'].get("pin")
                            totp_secret = broker_info['credentials'].get("totp_secret")
                            session = ar.angelone_get_session(api_key, user_id, pin, totp_secret)
                            auth_token = session["auth_token"]
                            interval = ar.number_to_interval(interval)
                            combined_df = ar.angelone_get_historical_data(
                                api_key, auth_token, session["obj"], "NSE",
                                instrument_key, interval
                            )

                    elif broker_name == "5paisa":
                        broker_info = next(
                            (b for b in selected_brokers if b['name'] == broker_key), None
                        )
                        if broker_info:
                            access_token = broker_info['credentials'].get("access_token")
                            combined_df = fp.fivepaisa_historical_data_fetch(
                                access_token, instrument_key, interval, 25
                            )

                except Exception as e:
                    logger_util.push_log(f"❌ Error fetching data for {symbol}: {e}", "error")
                    continue

                if combined_df is None or combined_df.empty:
                    logger_util.push_log(f"⚠️ No data returned for {symbol}, skipping.", "warning")
                    continue

                logger_util.push_log(f"✅ Data ready for {symbol}")
                indicators_df = ind.all_indicators(combined_df, strategy)
                row = indicators_df.tail(1).iloc[0]

                cols = indicators_df.columns.tolist()
                formatted = " | ".join([f"{c}:{row[c]}" for c in cols])
                logger_util.push_log(f"📊 Indicators: {formatted}")

                try:
                    creds = next(
                        (b["credentials"] for b in selected_brokers if b["name"] == broker_key),
                        None
                    )
                    lots = stock.get("lots")
                    target_pct = stock.get("target_percentage")

                    if broker_name == "upstox":
                        us.upstox_trade_conditions_check(
                            lots, target_pct, indicators_df.tail(1),
                            creds, company, symbol, exchange_type, strategy
                        )
                    elif broker_name == "zerodha":
                        zr.zerodha_trade_conditions_check(
                            lots, target_pct, indicators_df.tail(1),
                            creds, symbol, strategy
                        )
                    elif broker_name == "angelone":
                        ar.angelone_trade_conditions_check(
                            session["obj"], auth_token, lots, target_pct,
                            indicators_df, creds, symbol, strategy
                        )
                    elif broker_name == "5paisa":
                        fp.fivepaisa_trade_conditions_check(
                            lots, target_pct, indicators_df, creds, stock, strategy
                        )

                except Exception as e:
                    logger_util.push_log(f"❌ Error executing trade for {symbol}: {e}", "error")

                del combined_df
                del indicators_df
                gc.collect()

            logger_util.push_log(f"✅ Trading cycle completed at {now_interval}")
            logger_util.push_log(f"⏳ Waiting for next interval at {next_interval}...")

            gsleep(1)

    logger_util.push_log("🏁 All active trades ended. Exiting trading loop.")
    gc.collect()
