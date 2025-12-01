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
    logger_util.push_log("⚠️ REDIS_URL not found, defaulting to localhost Redis (for dev).", level = "warning", user_id = "admin", log_type = "trading")
    REDIS_URL = "redis://localhost:6379"

logger_util.push_log(f"🚀 Initializing Celery with broker: {REDIS_URL}", level = "warning", user_id = "admin", log_type = "trading")

# GLOBAL redis client (used for control/sets)
r = redis.StrictRedis.from_url(REDIS_URL, decode_responses=True)

stock_map = {
    "RELIANCE INDUSTRIES LTD": "RELIANCE",
    "HDFC BANK LTD": "HDFCBANK",
    "ICICI BANK LTD.": "ICICIBANK",
    "INFOSYS LIMITED": "INFY",
    "TATA CONSULTANCY SERV LT": "TCS",
    "STATE BANK OF INDIA": "SBIN",
    "AXIS BANK LIMITED": "AXISBANK",
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

# keep same broker maps (lowercase names used internally)
broker_map = {"u": "upstox", "z": "zerodha", "a": "angelone", "f": "5paisa", "g": "groww"}
reverse_stock_map = {}

celery_app.conf.task_track_started = True
celery_app.conf.worker_concurrency = 4

# ---- helper: per-user redis key names ----
def get_active_key(user_id: str) -> str:
    return f"active_trades:{user_id}"

def get_kill_key(user_id: str) -> str:
    return f"kill_trading:{user_id}"

# ---- Celery task entrypoint (bound) ----
@celery_app.task(bind=True)
def start_trading_loop(self, config: dict):

    logger_util.push_log("🚀 Trading engine started inside Celery worker.", level = "info", user_id = "admin", log_type = "trading")
    try:
        user_id = config.get("user_id")
        trading_params = config.get("tradingParameters", [])
        selected_brokers = config.get("selectedBrokers", [])
        logger_util.push_log(f"🚀 Trading started for user_id={user_id}", level = "info", user_id = "admin", log_type = "trading")
    except Exception as e:
        logger_util.push_log(f"❌ Invalid task config: {e}", level = "error", user_id = "admin", log_type = "trading")
        return

    try:
        run_trading_logic_for_all(user_id, trading_params, selected_brokers)
    except Exception as e:
        logger_util.push_log(f"💥 Trading loop crashed: {e}", level = "error", user_id = "admin", log_type = "trading")
        return

    logger_util.push_log("✅ Trading engine task finished successfully.", level = "info", user_id = "admin", log_type = "trading")


def run_trading_logic_for_all(user_id, trading_parameters, selected_brokers):
    """
    Main trading loop for a single user.
    This function preserves your original logic but uses per-user Redis keys
    instead of a global in-memory active_trades dict.
    """
    logger_util.push_log("✅ Trading loop started for all selected stocks",level = "info", user_id = user_id, log_type = "trading")
    logger_util.push_log("⏳ Starting trading cycle setup...", level = "info", user_id = user_id, log_type = "trading")

    # Build per-user active set
    active_key = get_active_key(user_id)
    # Use the global redis client `r` defined at module top
    try:
        symbols = [s['symbol_value'] for s in trading_parameters if s.get("symbol_value")]
    except Exception:
        symbols = []

    if symbols:
        # Reset existing user-specific set and add initial symbols
        try:
            r.delete(active_key)
            r.sadd(active_key, *symbols)
            logger_util.push_log(f"🟢 Active trades initialized for {user_id}: {', '.join(symbols)}", level = "info", user_id = user_id, log_type = "trading")
        except Exception as e:
            logger_util.push_log(f"❌ Redis error initializing active trades for {user_id}: {e}", level ="error", user_id = user_id, log_type = "trading")

        broker_key = stock.get('broker')
        broker_name = broker_map.get(broker_key, "unknown")
        symbol = stock.get('symbol_value')
        name = stock.get('symbol_key')
        strategy = stock.get('strategy')
        company = stock.get("symbol_key", symbol)
        interval = stock.get('interval')
        exchange_type = stock.get('type')
        # STEP 1: Fetch instrument keys (only for symbols still marked active for this user)
        for stock in trading_parameters:
            # Skip symbols not active for this user
            try:
                if not r.sismember(active_key, stock.get('symbol_value')):
                    continue
            except Exception:
                # Redis issue: be conservative and continue
                continue

        logger_util.push_log(
            f"🔑 Fetching instrument key for company : {company}, Name : {name} symbol :{symbol} via Broker : {broker_name}...", level = "info", user_id = user_id, log_type = "trading")

        instrument_key = None
        try:
            if exchange_type == "EQUITY":
                if broker_name == "upstox":
                    instrument_key = us.upstox_equity_instrument_key(user_id, company)
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
                matched = us.upstox_commodity_instrument_key(user_id, name, symbol)
                # matched may be a dataframe — keep original behavior
                instrument_key = matched['instrument_key'].iloc[0]

            # Set instrument_key if found; else mark symbol inactive for this user
            if instrument_key:
                stock['instrument_key'] = instrument_key
                logger_util.push_log(f"✅ Found instrument key {instrument_key} for {symbol}", level = "info", user_id = user_id, log_type = "trading")
            else:
                logger_util.push_log(f"⚠️ No instrument key found for {symbol}, skipping.", level = "warning", user_id = user_id, log_type = "trading")
                try:
                    r.srem(active_key, symbol)
                except Exception:
                    pass

        except Exception as e:
            logger_util.push_log(f"❌ Error code 1001 : Error fetching instrument key for {symbol}", level="error", user_id=user_id,log_type="trading")
            logger_util.push_log(f"❌ Error code 1001 : Error fetching instrument key for {symbol}: {e}", level = "error", user_id = "admin", log_type = "trading")
            try:
                r.srem(active_key, symbol)
            except Exception:
                pass

    # STEP 2: Interval setup
    if not trading_parameters:
        logger_util.push_log("⚠️ No trading parameters supplied. Exiting.", level = "warning", user_id = user_id, log_type = "trading")
        return

    interval = trading_parameters[0].get("interval", "1minute")
    now_interval, next_interval = nni.round_to_next_interval(interval)
    logger_util.push_log(f"🕓 Present Interval Start: {now_interval}, Next Interval: {next_interval}", level = "info", user_id = user_id, log_type = "trading")

    # Ensure redis client (use module-level r)
    # (r already defined at top using REDIS_URL)
    try:
        # Recompute symbol list from active set for user
        active_symbols = list(r.smembers(active_key))
    except Exception as e:
        logger_util.push_log(f"❌ Error Code 1002 : Error reading active symbols for {user_id}", level="error", user_id=user_id,log_type="trading")
        logger_util.push_log(f"❌ Error code 1002 : Redis error reading active symbols for {user_id}: {e}", level = "error", user_id = "admin", log_type = "trading")
        active_symbols = [s["symbol_value"] for s in trading_parameters if s.get("symbol_value")]

    symbols = [s for s in active_symbols if s]  # cleaned list
    if not symbols:
        logger_util.push_log("⚠️ No valid symbols to start trading. Exiting.", level = "warning", user_id = user_id, log_type = "trading")
        return

    # Small pause for any async initialization
    time.sleep(0.5)

    # STEP 3: Trading loop
    while True:
        # Refresh active symbols for this user
        try:
            active_symbols = set(r.smembers(active_key))
        except Exception as e:
            logger_util.push_log(f"❌ Error Code 1003 : Error reading active symbols for {user_id}", level="error",user_id=user_id, log_type="trading")
            logger_util.push_log(f"❌ Error code 1003 : Redis error reading active symbols for {user_id}: {e}",level="error", user_id="admin", log_type="trading")
            # On Redis error, break to avoid uncontrolled looping
            break

        # Filter trading_parameters by currently active symbols for this user
        trading_parameters = [
            s for s in trading_parameters if s.get("symbol_value") in active_symbols
        ]

        # If no active trades remain for this user, exit
        # ⭐ NEW: Global kill flag (backend stop all)
        kill_key = get_kill_key(user_id)
        if r.get(kill_key) == "1":
            logger_util.push_log(f"🛑 STOP SIGNAL RECEIVED for {user_id} — exiting trading loop.", level = "info",  user_id = user_id, log_type = "trading")
            r.delete(kill_key)
            r.delete(active_key)
            break

        # ⭐ Existing logic: no active trades
        if not trading_parameters or len(active_symbols) == 0 or r.scard(active_key) == 0:
            logger_util.push_log(f"🏁 All trades stopped for {user_id} — exiting trading loop.", level = "info", user_id = user_id, log_type = "trading")
            break

        now = datetime.datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")

        if now >= next_interval:
            # Recompute intervals
            now_interval, next_interval = nni.round_to_next_interval(interval)
            logger_util.push_log(f"⏱ New interval reached: {now_interval}", level = "info", user_id = user_id, log_type = "trading")

            # Loop through each active stock and perform fetch, indicators, trade checks
            for stock in trading_parameters:
                symbol = stock.get('symbol_value')
                # Quick check if this symbol has been deactivated mid-run
                if not r.sismember(active_key, symbol):
                    continue

                broker_key = stock.get('broker')
                broker_name = broker_map.get(broker_key)
                company = stock.get('symbol_key')
                interval = stock.get('interval')
                instrument_key = stock.get('instrument_key')
                strategy = stock.get('strategy')
                exchange_type = stock.get('type')
                tick_size = stock.get('tick_size')

                logger_util.push_log(f"🕯 Fetching candles for {symbol}-{company} from {broker_name}", level = "info", user_id = user_id, log_type = "trading")

                combined_df = None
                # local holders for session/auth_token used in different broker flows
                session = None
                auth_token = None

                try:
                    if broker_name == "upstox":
                        access_token = next(
                            (b['credentials']['access_token']
                             for b in selected_brokers if b['name'] == broker_key),
                            None
                        )
                        if access_token:
                            hdf = us.upstox_fetch_historical_data_with_retry( user_id, access_token, instrument_key, interval)
                            idf = us.upstox_fetch_intraday_data(user_id, access_token, instrument_key, interval)
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
                            user_id_cred = broker_info['credentials'].get("user_id")
                            pin = broker_info['credentials'].get("pin")
                            totp_secret = broker_info['credentials'].get("totp_secret")
                            session = ar.angelone_get_session(api_key, user_id_cred, pin, totp_secret)
                            # session may include auth_token and obj (used in execution)
                            auth_token = session.get("auth_token") if isinstance(session, dict) else None
                            interval = ar.number_to_interval(interval)
                            combined_df = ar.angelone_get_historical_data(
                                api_key, auth_token, session.get("obj") if isinstance(session, dict) else None, "NSE",
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
                    logger_util.push_log(f"❌ Error code 1004 : Error fetching data for {symbol}", level="error", user_id=user_id,log_type="trading")
                    logger_util.push_log(f"❌ Error code 1004 : Error fetching data for {symbol}: {e}", level = "error", user_id = "admin", log_type = "trading")
                    # If fetching fails repeatedly for a symbol, consider deactivating it
                    # but to mimic old logic, we simply skip this iteration.
                    continue

                if combined_df is None or combined_df.empty:
                    logger_util.push_log(f"⚠️ No data returned for {symbol}, skipping.", level = "warning", user_id = user_id, log_type = "trading")
                    continue

                logger_util.push_log(f"✅ Data ready for {symbol}", level = "info", user_id = user_id, log_type = "trading")
                indicators_df = ind.all_indicators(combined_df, strategy)
                # if indicators_df might be empty after dropna, guard:
                if indicators_df is None or indicators_df.empty:
                    logger_util.push_log(f"⚠️ Indicators empty for {symbol}, skipping.", level = "warning", user_id = user_id, log_type = "trading")
                    continue

                row = indicators_df.tail(1).iloc[0]

                cols = indicators_df.columns.tolist()
                formatted = " | ".join([f"{c}:{row[c]}" for c in cols])
                logger_util.push_log(f"📊 Indicators: {formatted}", level = "info", user_id = user_id, log_type = "trading")

                try:
                    creds = next(
                        (b["credentials"] for b in selected_brokers if b["name"] == broker_key),
                        None
                    )
                    lots = stock.get("lots")
                    target_pct = stock.get("target_percentage")

                    if broker_name == "upstox":
                        # Upstox execution uses access_token inside creds
                        us.upstox_trade_conditions_check(user_id,
                            lots, target_pct, indicators_df.tail(1),
                            creds, company, symbol, exchange_type, strategy
                        )
                    elif broker_name == "zerodha":
                        zr.zerodha_trade_conditions_check(
                            lots, target_pct, indicators_df.tail(1),
                            creds, symbol, strategy
                        )
                    elif broker_name == "angelone":
                        # Ensure session/auth_token used if available; original code used session/auth_token variables
                        try:
                            ar.angelone_trade_conditions_check(
                                session.get("obj") if isinstance(session, dict) else session,
                                auth_token,
                                lots, target_pct,
                                indicators_df, creds, symbol, strategy
                            )
                        except Exception as e:
                            logger_util.push_log(f"❌ Error code 1005 : AngelOne trade execution error for {symbol}", level="error",user_id=user_id, log_type="trading")
                            logger_util.push_log(f"❌ Error code 1005 : AngelOne trade execution error for {symbol}: {e}", level = "error", user_id = "admin", log_type = "trading")
                    elif broker_name == "5paisa":
                        fp.fivepaisa_trade_conditions_check(
                            lots, target_pct, indicators_df, creds, stock, strategy
                        )

                except Exception as e:
                    logger_util.push_log(f"❌ Error code 1006 : Error executing trade for {symbol}", level="error", user_id=user_id,log_type="trading")
                    logger_util.push_log(f"❌ Error code 1006 : Error executing trade for {symbol}: {e}", level = "error", user_id = "admin", log_type = "trading")

                # Clean up per-symbol memory
                del combined_df
                del indicators_df
                gc.collect()

            # END FOR each stock
            logger_util.push_log(f"✅ Trading cycle completed at {now_interval}", level = "info",  user_id = user_id, log_type = "trading")
            logger_util.push_log(f"⏳ Waiting for next interval at {next_interval}...", level = "info", user_id = user_id, log_type = "trading")

            gsleep(1)

    # END WHILE loop
    logger_util.push_log(f"🏁 All active trades ended for {user_id}. Exiting trading loop.", level = "info", user_id = user_id, log_type = "trading")
    gc.collect()
