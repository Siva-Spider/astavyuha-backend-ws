# trading_engine_routes.py
from fastapi import APIRouter, HTTPException, Request, Query
from fastapi.responses import FileResponse, JSONResponse
from pathlib import Path
import backend.logger_util as logger_util 
import get_lot_size as ls
from find_positions_with_symbol import find_positions_for_symbol
import json
import traceback

router = APIRouter(prefix="/api", tags=["Trading"])

DATA_DIR = Path("data")
LATEST_LINK_FILENAME = "complete.csv.gz"
broker_sessions = {}
broker_map = {
    "u": "Upstox",
    "z": "Zerodha",
    "a": "AngelOne",
    "g": "Groww",
    "5": "5paisa"
}

# ✅ GET /api/instruments/latest
@router.get("/instruments/latest")
async def get_latest_instruments():
    try:
        from upstox_instrument_manager import update_instrument_file
        file_path = DATA_DIR / LATEST_LINK_FILENAME
        if not file_path.exists():
            update_instrument_file()
            if not file_path.exists():
                raise HTTPException(
                    status_code=503,
                    detail="Instrument file not yet available. Please wait for daily update."
                )
        return FileResponse(
            path=file_path,
            filename="complete.csv.gz",
            media_type="application/gzip"
        )
    except Exception as e:
        logger_util.push_log(f"❌ Error serving instrument file: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))


# ✅ POST /api/connect-broker
@router.post("/connect-broker")
async def connect_broker(request: Request):
    try:
        import Upstox as us
        import Zerodha as zr
        import Groww as gr
        import Fivepaisa as fp
        import AngelOne as ar

        data = await request.json()
        brokers_data = data.get("brokers", [])
        responses = []

        for broker_item in brokers_data:
            broker_key = broker_item.get("name")
            creds = broker_item.get("credentials")
            broker_name = broker_map.get(broker_key)
            profile = None
            balance = None
            message = "Broker not supported or credentials missing."
            status = "failed"

            try:
                if broker_name == "Upstox":
                    access_token = creds.get("access_token")
                    profile = us.upstox_profile(access_token)
                    balance = us.upstox_balance(access_token)
                    if profile and balance:
                        status = "success"
                        message = "Connected successfully."
                    else:
                        message = "Connection failed. Check access token."

                elif broker_name == "Zerodha":
                    api_key = creds.get("api_key")
                    access_token = creds.get("access_token")
                    profile = zr.zerodha_get_profile(api_key, access_token)
                    balance = zr.zerodha_get_equity_balance(api_key, access_token)
                    if profile and balance:
                        status = "success"
                        message = "Connected successfully."
                    else:
                        message = "Connection failed. Check credentials."

                elif broker_name == "AngelOne":
                    api_key = creds.get("api_key")
                    user_id = creds.get("user_id")
                    pin = creds.get("pin")
                    totp_secret = creds.get("totp_secret")
                    obj, refresh_token, auth_token, feed_token = ar.angelone_connect(
                        api_key, user_id, pin, totp_secret
                    )
                    profile, balance = ar.angelone_fetch_profile_and_balance(obj, refresh_token)
                    if profile and balance:
                        status = "success"
                        message = "Connected successfully."
                        broker_sessions[broker_name] = {
                            "obj": obj,
                            "refresh_token": refresh_token,
                            "auth_token": auth_token,
                            "feed_token": feed_token
                        }
                    else:
                        message = "Connection failed. Check credentials."

                elif broker_name == "5paisa":
                    app_key = creds.get("app_key")
                    access_token = creds.get("access_token")
                    client_code = creds.get("client_id")
                    profile = {"User Name": client_code}
                    balance = fp.fivepaisa_get_balance(app_key, access_token, client_code)
                    if profile and balance:
                        status = "success"
                        message = "Connected successfully."
                    else:
                        message = "Connection failed. Check credentials."

                elif broker_name == "Groww":
                    api_key = creds.get("api_key")
                    access_token = creds.get("access_token")
                    if api_key and access_token:
                        profile = {"User Name": f"Dummy {broker_name} User"}
                        balance = {"Available Margin": "10000.00"}
                        status = "success"
                        message = "Connected successfully."
                    else:
                        message = "Connection failed. Missing API key or token."

            except Exception as e:
                status = "failed"
                message = f"An error occurred: {str(e)}"
                logger_util.push_log(message, "error")

            responses.append({
                "broker": broker_name,
                "broker_key": broker_key,
                "status": status,
                "message": message,
                "profileData": {
                    "profile": profile,
                    "balance": balance,
                    "status": status,
                    "message": message
                }
            })

        return JSONResponse(content=responses)
    except Exception as e:
        logger_util.push_log(f"❌ Error in connect_broker: {e}", "error")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# ✅ POST /api/get-lot-size
@router.post("/get-lot-size")
async def get_lot_size(request: Request):
    try:
        data = await request.json()
        symbol_key = data.get("symbol_key")
        symbol_value = data.get("symbol_value")
        type_ = data.get("type")

        if not symbol_key:
            raise HTTPException(status_code=400, detail="Stock symbol is required.")
        if type_ == "EQUITY":
            lot_size, tick_size = ls.lot_size(symbol_key)
        elif type_ == "COMMODITY":
            lot_size, tick_size = ls.commodity_lot_size(symbol_key, symbol_value)
        else:
            lot_size, tick_size = None, None

        if lot_size:
            return {"lot_size": lot_size, "tick_size": tick_size, "symbol": symbol_key}
        else:
            return JSONResponse(status_code=404, content={"message": "Lot size not found."})

    except Exception as e:
        msg = f"Error in get_lot_size: {e}"
        logger_util.push_log(msg, "error")
        raise HTTPException(status_code=500, detail=str(e))


# ✅ GET version for lot size (to match Flask behavior)
@router.get("/get-lot-size")
async def get_lot_size_get(
    symbol_key: str = Query(...),
    symbol_value: str = Query(None),
    type_: str = Query("EQUITY")
):
    try:
        if not symbol_key:
            raise HTTPException(status_code=400, detail="Stock symbol is required.")
        if type_ == "EQUITY":
            lot_size, tick_size = ls.lot_size(symbol_key)
        elif type_ == "COMMODITY":
            lot_size, tick_size = ls.commodity_lot_size(symbol_key, symbol_value)
        else:
            lot_size, tick_size = None, None

        if lot_size:
            return {"lot_size": lot_size, "tick_size": tick_size, "symbol": symbol_key}
        else:
            return JSONResponse(status_code=404, content={"message": "Lot size not found."})
    except Exception as e:
        msg = f"Error in get_lot_size (GET): {e}"
        logger_util.push_log(msg, "error")
        raise HTTPException(status_code=500, detail=str(e))
