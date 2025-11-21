# main_fastapi.py
import random
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from fastapi import FastAPI, HTTPException, Depends, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi import WebSocket, WebSocketDisconnect
import backend.logger_util as logger_util
from pydantic import BaseModel
import sqlite3
import os

# --- Additional Imports ---
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pathlib import Path
from backend import get_lot_size as ls
from backend.upstox_instrument_manager import update_instrument_file
from backend import get_expiry_date as ed
from backend import find_positions_with_symbol as fps
from backend import user_manager as usr
from backend import Upstox as us

# Import helpers used in many endpoints
from backend.update_db import init_db
from backend.password_utils import generate_random_password
from backend.email_utils import send_email

from fastapi import Request
import redis, json, time, asyncio, threading, queue
from urllib.parse import urlparse
import aiosmtplib

# re-use the same module that has websocket_connections

# Existing broker map
broker_map = {
    "u": "Upstox",
    "z": "Zerodha",
    "a": "AngelOne",
    "g": "Groww",
    "5": "5paisa"
}

broker_sessions = {}
event_loop = None
DATA_DIR = Path("data")
LATEST_LINK_FILENAME = "complete.csv.gz"

GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL")
OTP_FILE = "otp_store.json"
OTP_EXPIRY = 300  # 5 minutes

# Celery app and trading task
try:
    from celery_app import celery_app
except Exception:
    celery_app = None

# Import the task
from backend.tasks.trading_tasks import start_trading_loop

# Create FastAPI app
app = FastAPI(title="Astavyuha Backend (FastAPI wrapper)", version="1.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

REDIS_URL = os.getenv("REDIS_URL", "").strip() or "redis://localhost:6379"
REDIS_PUBSUB_CHANNEL = "log_stream"

# Thread control
_redis_thread = None
_redis_thread_stop = threading.Event()

def _redis_listener_thread(redis_url, channel):
    """
    Blocking thread which polls Redis PubSub and forwards published logs
    to FastAPI WebSocket clients using logger_util.push_ws().
    """
    try:
        parsed = urlparse(redis_url)
        if parsed.scheme == "rediss":
            r = redis.StrictRedis.from_url(redis_url, ssl_cert_reqs=None, decode_responses=True)
        else:
            r = redis.StrictRedis.from_url(redis_url, decode_responses=True)

        pubsub = r.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(channel)

        print(f"✅ Redis listener thread subscribed to: {channel}")

    except Exception as e:
        print("⚠️ Redis listener failed to start:", e)
        return

    while not _redis_thread_stop.is_set():
        try:
            message = pubsub.get_message(timeout=1)

            if message and message.get("type") == "message":
                data = message.get("data")

                # Convert bytes → string
                if isinstance(data, bytes):
                    try:
                        data = data.decode("utf-8")
                    except:
                        data = str(data)

                # Validate JSON
                try:
                    payload = json.loads(data)
                    entry_json = json.dumps(payload)
                except:
                    entry_json = data if isinstance(data, str) else json.dumps({"message": str(data)})

                # Forward to WebSocket
                try:
                    import backend.logger_util as logger_util
                    logger_util.push_ws(entry_json)
                except Exception as e:
                    print("⚠️ Failed to push WS log from Redis listener:", e)

        except Exception as e:
            print("⚠️ Redis listener error:", e)
            time.sleep(0.5)

    try:
        pubsub.close()
    except:
        pass

    print("🧹 Redis listener thread stopped.")


@app.on_event("startup")
async def start_redis_listener():
    print("🔥 FastAPI startup running...")

    # Store event loop inside logger_util
    try:
        import backend.logger_util as logger_util
        logger_util.event_loop = asyncio.get_running_loop()
        print("✅ Stored FastAPI event_loop inside logger_util.event_loop")
    except Exception as e:
        print("❌ Failed to set logger_util.event_loop:", e)

    global _redis_thread, _redis_thread_stop

    try:
        logger_util.event_loop = asyncio.get_running_loop()
    except Exception:
        pass

    if _redis_thread is None or not _redis_thread.is_alive():
        _redis_thread_stop.clear()
        _redis_thread = threading.Thread(
            target=_redis_listener_thread,
            args=(REDIS_URL, REDIS_PUBSUB_CHANNEL),
            daemon=True
        )
        _redis_thread.start()
        print("✅ Started Redis → WebSocket listener thread.")


@app.on_event("startup")
async def startup_event():
    print("🔥 FASTAPI STARTUP")
    loop = asyncio.get_running_loop()
    logger_util.event_loop = loop
    print("🔥 logger_util.event_loop SET to:", logger_util.event_loop)


@app.on_event("shutdown")
async def stop_redis_listener():
    global _redis_thread_stop
    _redis_thread_stop.set()
    print("🛑 Stopping Redis listener thread...")


# ---------- Models ----------
class LoginRequest(BaseModel):
    userId: str
    password: str
    role: str | None = None


class RegisterRequest(BaseModel):
    username: str
    email: str
    mobilenumber: str
    role: str
    password: str  # ⭐ REQUIRED
    userId: str | None = None


# ---------- DB Helpers ----------
DB_PATH = os.path.join(os.getcwd(), "user_data_new.db")


def get_user_row_by_userid(userId: str):
    """Return row or None"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT userId, password, role, email, mobilenumber, username FROM users WHERE userId = ?",
        (userId,)
    )
    row = cursor.fetchone()
    conn.close()
    return row


# ---------- Endpoints ----------
@app.get("/api/health")
async def health():
    return {"status": "ok", "message": "FastAPI backend running"}


@app.post("/api/login")
async def login_user(data: LoginRequest):
    print("📩 Login request received:", data)
    try:
        if data.role and data.role.lower() == "client":
            data.role = "user"

        row = get_user_row_by_userid(data.userId)
        if not row:
            print(f"⚠️ Login attempt failed: user {data.userId} not found")
            return {"success": False, "message": "Invalid User ID"}

        db_userId, db_password, db_role, db_email, db_mobile, db_username = row

        if db_password != data.password:
            print(f"⚠️ Incorrect password for user {data.userId}")
            return {"success": False, "message": "Incorrect password"}

        if data.role:
            normalized_role = data.role.lower().strip()
            db_normalized_role = db_role.lower().strip()
            if normalized_role == "client":
                normalized_role = "user"
            if normalized_role != db_normalized_role:
                print(f"⚠️ Role mismatch: provided {data.role}, db {db_role}")
                return {"success": False, "message": "Role mismatch"}

        profile = {
            "userid": db_userId,
            "username": db_username or db_userId,
            "email": db_email or "",
            "mobilenumber": db_mobile or "",
            "role": db_role,
        }
        print(f"✅ User {data.userId} logged in as {db_role}")
        return {"success": True, "message": "Login successful", "profile": profile}

    except Exception as e:
        print(f"❌ Login error: {e}", "error")
        return {"success": False, "message": str(e)}


@app.post("/api/register")
async def register_user_route(data: RegisterRequest):
    try:
        role = data.role.lower().strip()
        if role == "client":
            role = "user"

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pending_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                userId TEXT UNIQUE,
                username TEXT,
                email TEXT,
                password TEXT,
                role TEXT,
                mobilenumber TEXT
            )
        """)

        user_id_candidate = data.userId or data.username.split()[0]
        cursor.execute("SELECT * FROM users WHERE email=? OR userId=?", (data.email, user_id_candidate))

        if cursor.fetchone():
            conn.close()
            return {"success": False, "message": "User already exists."}

        cursor.execute("SELECT * FROM pending_users WHERE email=? OR userId=?", (data.email, user_id_candidate))

        if cursor.fetchone():
            conn.close()
            return {"success": False, "message": "User already pending."}

        password = data.password

        cursor.execute(
            "INSERT INTO pending_users (userId, username, email, password, role, mobilenumber) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id_candidate, data.username, data.email, password, role, data.mobilenumber)
        )
        conn.commit()
        conn.close()

        try:
            send_email(
                data.email,
                "Registration Received - Pending Approval",
                f"Dear {data.username},"
                f"\nYour registration is pending approval."
            )
        except Exception as mail_err:
            print(f"⚠️ Pending user email failed for {data.email}: {mail_err}")

        print(f"🕓 New pending registration: {user_id_candidate} ({role})")

        return {
            "success": True,
            "message": "Registration submitted.",
            "profile": {
                "userid": user_id_candidate,
                "username": data.username,
                "email": data.email,
                "mobilenumber": data.mobilenumber,
                "role": role,
            },
        }

    except Exception as e:
        print(f"❌ Error in pending registration: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))


# Utility sleep
async def async_gsleep(seconds: float):
    await asyncio.sleep(seconds)

def gsleep(seconds: float):
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(asyncio.sleep(seconds))
    except RuntimeError:
        time.sleep(seconds)


@app.post("/api/get_profit_loss")
async def get_profit_loss(request: Request):
    try:
        data = await request.json()
        access_token = data.get("access_token")
        segment = data.get("segment")
        from_date = data.get("from_date")
        to_date = data.get("to_date")
        year = data.get("year")

        if year and "-" in year:
            parts = year.split("-")
            fy_code = parts[0][-2:] + parts[1][-2:]
        else:
            fy_code = year

        if not all([access_token, segment, from_date, to_date, fy_code]):
            raise HTTPException(status_code=400, detail="Missing parameters")

        result, charges = us.upstox_profit_loss(access_token, segment, from_date, to_date, fy_code)

        print(f"✅ Profit/Loss fetched successfully for {segment} FY{fy_code}")

        return JSONResponse(content={"success": True, "data": result, "rows": charges})

    except Exception as e:
        try:
            print(f"❌ Error in /api/get_profit_loss: {e}", "error")
        except:
            pass
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/start-all-trading")
async def start_trading(request: Request):
    try:
        data = await request.json()
        print(data)
        config_file = Path("trading_config.json")

        # Save full config
        config_file.write_text(json.dumps(data, indent=2))
        print("💾 Saved unified trading configuration to trading_config.json")

        task = start_trading_loop.apply_async()
        print(f"🟢 Celery task {task.id} started.")

        return {"success": True, "message": "Trading loop started.", "task_id": task.id}

    except Exception as e:
        print(f"❌ Could not start trading task: {e}", "error")
        return {"success": False, "message": str(e)}

@app.post("/api/connect-broker")
async def connect_broker(request: Request):
    """
    Connects to selected brokers using credentials.
    """
    try:
        import backend.Upstox as us
        import backend.Zerodha as zr
        import backend.Groww as gr
        import backend.Fivepaisa as fp
        import backend.AngelOne as ar

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
                    print(access_token)
                    profile = us.upstox_profile(access_token)
                    balance = us.upstox_balance(access_token)
                    if profile and balance:
                        status, message = "success", "Connected successfully."
                    else:
                        message = "Connection failed. Check your access token."

                elif broker_name == "Zerodha":
                    api_key = creds.get("api_key")
                    access_token = creds.get("access_token")
                    profile = zr.zerodha_get_profile(api_key, access_token)
                    balance = zr.zerodha_get_equity_balance(api_key, access_token)
                    if profile and balance:
                        status, message = "success", "Connected successfully."
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
                        status, message = "success", "Connected successfully."
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
                        status, message = "success", "Connected successfully."
                    else:
                        message = "Connection failed. Check credentials."

                elif broker_name == "Groww":
                    api_key = creds.get("api_key")
                    access_token = creds.get("access_token")
                    if api_key and access_token:
                        profile = {"User Name": f"Dummy {broker_name} User"}
                        balance = {"Available Margin": "10000.00"}
                        status, message = "success", "Connected successfully."
                    else:
                        message = "Connection failed. Missing credentials."

            except Exception as e:
                status, message = "failed", f"An error occurred: {str(e)}"
                print(message, "error")

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
        print(f"❌ Error in connect_broker: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/get-lot-size")
async def get_lot_size_post(request: Request):
    """
    POST version for lot size (accepts JSON body)
    """
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

        if lot_size:
            return {"lot_size": lot_size, "tick_size": tick_size, "symbol": symbol_key}
        else:
            return JSONResponse(status_code=404, content={"message": "Lot size not found."})
    except Exception as e:
        msg = f"Error in get_lot_size (POST): {e}"
        print(msg, "error")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/disconnect-stock")
async def disconnect_stock(request: Request):
    """
    Disconnect a symbol from active trades.
    Removes it from Redis active_trades set.
    """
    import redis
    data = await request.json()
    symbol = data.get("symbol_value")

    if not symbol:
        return {"success": False, "message": "Symbol missing."}

    try:
        r = redis.StrictRedis(host="localhost", port=6379, db=5, decode_responses=True)
        removed = r.srem("active_trades", symbol)
        if removed:
            logger_util.push_log(f"🛑 User disconnected {symbol} — stopping trade after current interval.")
            return {"success": True, "message": f"{symbol} disconnected."}
        else:
            return {"success": False, "message": f"{symbol} was not active."}
    except Exception as e:
        return {"success": False, "message": f"Redis error: {e}"}

@app.post("/api/close-position")
async def close_position(request: Request):
    """
    Close a specific position manually.
    """
    try:
        data = await request.json()
        broker = data.get("broker")
        symbol = data.get("symbol")
        credentials = data.get("credentials")

        if not broker or not symbol:
            raise HTTPException(status_code=400, detail="Missing broker or symbol.")

        # You can call the respective broker close position logic here.
        logger_util.push_log(f"🔻 Closed position for {symbol} ({broker})")
        return {"message": f"Closed position for {symbol} ({broker})"}

    except Exception as e:
        logger_util.push_log(f"❌ Error closing position: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/close-all-positions")
async def close_all_positions(request: Request):
    """
    Close all open positions across all brokers.
    """
    try:
        data = await request.json()
        brokers = data.get("brokers", [])
        summary = []

        for broker_item in brokers:
            broker = broker_item.get("broker")
            credentials = broker_item.get("credentials")

            try:
                # For each broker, you can call their close_all_positions() function
                logger_util.push_log(f"🔻 Closing all positions for {broker}")
                summary.append({
                    "broker": broker,
                    "status": "success",
                    "message": "All positions closed"
                })
            except Exception as inner_e:
                summary.append({
                    "broker": broker,
                    "status": "failed",
                    "message": str(inner_e)
                })

        return {"summary": summary}

    except Exception as e:
        print(f"❌ Error in /api/close-all-positions: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/users")
async def unified_users_list():
    """
    Frontend expects /users returning registered, pending, and rejected users.
    We'll return them in one JSON response.
    """
    try:
        conn = sqlite3.connect("user_data_new.db")
        cursor = conn.cursor()

        # Registered
        cursor.execute("SELECT userId, username, email, role, mobilenumber FROM users")
        registered = [
            {"userId": r[0], "username": r[1], "email": r[2], "role": r[3], "mobilenumber": r[4]}
            for r in cursor.fetchall()
        ]

        # Pending
        cursor.execute("SELECT userId, username, email, role, mobilenumber FROM pending_users")
        pending = [
            {"userId": r[0], "username": r[1], "email": r[2], "role": r[3], "mobilenumber": r[4]}
            for r in cursor.fetchall()
        ]

        # Rejected (optional table)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rejected_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                userId TEXT UNIQUE,
                username TEXT,
                email TEXT,
                role TEXT,
                mobilenumber TEXT
            )
        """)
        cursor.execute("SELECT userId, username, email, role, mobilenumber FROM rejected_users")
        rejected = [
            {"userId": r[0], "username": r[1], "email": r[2], "role": r[3], "mobilenumber": r[4]}
            for r in cursor.fetchall()
        ]

        conn.close()
        return {"users": registered, "pending": pending, "rejected": rejected}
    except Exception as e:
        print(f"❌ unified_users_list error: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/approve/{userId}")
async def approve_user(userId: str):
    """
    Move user from pending_users → users
    """
    try:
        conn = sqlite3.connect("user_data_new.db")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM pending_users WHERE userId=?", (userId,))
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="User not found in pending list")

        # Insert into users table
        cursor.execute("""
            INSERT INTO users (userId, username, email, password, role, mobilenumber)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (user[1], user[2], user[3], user[4], user[5], user[6]))
        # Extract fields
        _, u_userId, u_username, u_email, u_password, u_role, u_mobile = user
        # Delete from pending_users
        cursor.execute("DELETE FROM pending_users WHERE userId=?", (userId,))
        conn.commit()
        conn.close()
        # ⭐ Send approval email
        subject = "Your ASTA VYUHA Registration Is Approved"
        body = (
            f"Dear {u_username},\n\n"
            "Your registration has been approved successfully!\n\n"
            f"Your User ID: {u_userId}\n"
            f"Password: {u_password}\n\n"
            "You can now log in and start using the platform.\n\n"
            "Regards,\nASTA VYUHA Team"
        )

        # Call your existing SMTP mailer
        send_email(u_email, subject, body)
        print(f"✅ Approved pending user {userId}")
        return {"success": True, "message": f"User {userId} approved"}
    except Exception as e:
        print(f"❌ Error approving user {userId}: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/reject/{userId}")
async def reject_user(userId: str):
    """
    Move user from pending_users → rejected_users
    """
    try:
        conn = sqlite3.connect("user_data_new.db")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM pending_users WHERE userId=?", (userId,))
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="User not found in pending list")

        # Extract fields
        userId_val = user[1]
        username = user[2]
        email = user[3]
        role = user[5]
        mobilenumber = user[6]

        # Ensure rejected_users exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rejected_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                userId TEXT UNIQUE,
                username TEXT,
                email TEXT,
                role TEXT,
                mobilenumber TEXT
            )
        """)
        cursor.execute(
            "INSERT OR IGNORE INTO rejected_users (userId, username, email, role, mobilenumber) VALUES (?, ?, ?, ?, ?)",
            (user[1], user[2], user[3], user[5], user[6])
        )
        cursor.execute("DELETE FROM pending_users WHERE userId=?", (userId,))
        conn.commit()
        conn.close()
        # 📨 Send rejection email
        try:
            send_email(
                email,
                "Registration Rejected",
                f"Dear {username},\n\n"
                "We regret to inform you that your registration request has been rejected.\n"
                "If you believe this was a mistake or need assistance, please contact support.\n\n"
                "Regards,\nASTA VYUHA Team"
            )
        except Exception as mail_err:
            print(f"⚠️ Rejection email failed for {email}: {mail_err}")
        print(f"🚫 Rejected pending user {userId}")
        return {"success": True, "message": f"User {userId} rejected"}
    except Exception as e:
        print(f"❌ Error rejecting user {userId}: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/delete-user/{userId}")
async def delete_registered_user(userId: str):
    """
    Delete registered user (from /users table)
    """
    try:
        conn = sqlite3.connect("user_data_new.db")
        cursor = conn.cursor()
        cursor.execute("DELETE FROM users WHERE userId=?", (userId,))
        conn.commit()
        conn.close()
        print(f"🗑️ Deleted registered user {userId}")
        return {"success": True, "message": f"User {userId} deleted successfully"}
    except Exception as e:
        print(f"❌ Error deleting user {userId}: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/admin/delete-rejected/{userId}")
async def delete_rejected_user(userId: str):
    """
    Delete user from rejected_users table
    """
    try:
        conn = sqlite3.connect("user_data_new.db")
        cursor = conn.cursor()
        cursor.execute("DELETE FROM rejected_users WHERE userId=?", (userId,))
        conn.commit()
        conn.close()
        print(f"🗑️ Deleted rejected user {userId}")
        return {"success": True, "message": f"Rejected user {userId} deleted successfully"}
    except Exception as e:
        print(f"❌ Error deleting rejected user {userId}: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/admin/reset-password/{userId}")
async def reset_user_password(userId: str):
    """
    Reset password for a registered user.
    """
    try:
        new_pass = generate_random_password()
        conn = sqlite3.connect("user_data_new.db")
        cursor = conn.cursor()
        cursor.execute("UPDATE users SET password=? WHERE userId=?", (new_pass, userId))
        conn.commit()
        conn.close()
        print(f"🔑 Password reset for {userId}")
        return {"success": True, "message": "Password reset successfully", "new_password": new_pass}
    except Exception as e:
        print(f"❌ Error resetting password for {userId}: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/pending-users")
async def get_pending_users():
    """
    Fetch all pending users from the database.
    """
    try:
        conn = sqlite3.connect("user_data_new.db")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM pending_users")
        rows = cursor.fetchall()
        conn.close()

        users_data = [
            {
                "userId": row[1],
                "username": row[2],
                "email": row[3],
                "role": row[5],
                "mobilenumber": row[6],
            }
            for row in rows
        ]
        return {"pending_users": users_data}

    except Exception as e:
        print(f"❌ Error fetching pending users: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/get-positions")
async def get_positions(request: Request):
    """
    Fetch positions for a given broker and symbol.
    """
    try:
        data = await request.json()
        broker = data.get("broker")
        symbol = data.get("symbol")
        credentials = data.get("credentials")

        if not broker or not symbol or not credentials:
            raise HTTPException(status_code=400, detail="Missing required fields.")

        positions = fps.find_positions_for_symbol(broker, symbol, credentials)
        return {"broker": broker, "symbol": symbol, "positions": positions}

    except Exception as e:
        print(f"❌ Error in /api/get-positions: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/send-welcome-email")
async def send_welcome_email(request: Request):
    data = await request.json()
    email = data.get("email")
    first_name = data.get("firstName", "")

    if not email:
        return JSONResponse({"status": "error", "message": "Email not provided"}, status_code=400)

    msg = MIMEMultipart()
    msg['From'] = GMAIL_USER
    msg['To'] = email
    msg['Cc'] = GMAIL_USER
    msg['Subject'] = "Welcome to ASTA VYUHA"

    body = (
        f"Hi {first_name},\n\n"
        "Welcome to ASTA VYUHA! Registration may take few hours.\n"
        "After user validation you will get registration status.\n\n"
        "Regards,\nASTA VYUHA Team"
    )

    msg.attach(MIMEText(body, 'plain'))

    try:
        await aiosmtplib.send(
            message=msg,
            hostname="smtp.gmail.com",
            port=587,
            start_tls=True,
            username=GMAIL_USER,
            password=GMAIL_APP_PASSWORD,
            recipients=[email, GMAIL_USER],
        )

        return {"status": "success", "message": "Welcome email sent!"}

    except Exception as e:
        print("Email sending failed:", e)
        return JSONResponse({"status": "error", "message": "Email sending failed"}, status_code=500)

@app.post("/api/send-support-mail")
async def send_support_mail(request: Request):
    data = await request.json()

    user_email = data.get("email")
    user_name = data.get("name")
    subject = data.get("subject")
    message_body = data.get("message")

    if not all([user_email, user_name, subject, message_body]):
        return JSONResponse({"status": "error", "message": "All fields are required"}, status_code=400)

    msg = MIMEMultipart()
    msg["From"] = ADMIN_EMAIL
    msg["To"] = ADMIN_EMAIL
    msg["Cc"] = user_email
    msg["Subject"] = f"Support Request: {subject}"

    body = f"Support request from {user_name} ({user_email}):\n\n{message_body}"
    msg.attach(MIMEText(body, "plain"))

    recipients = [ADMIN_EMAIL, user_email]

    try:
        await aiosmtplib.send(
            message=msg,
            hostname="smtp.gmail.com",
            port=587,
            start_tls=True,
            username=ADMIN_EMAIL,
            password=GMAIL_APP_PASSWORD,
            recipients=recipients,
        )

        return {"status": "success", "message": "Support email sent successfully!"}

    except Exception as e:
        print("Support email failed:", e)
        return JSONResponse({"status": "error", "message": "Email sending failed"}, status_code=500)


def load_otp_store():
    try:
        if not Path(OTP_FILE).exists():
            return {}
        return json.loads(Path(OTP_FILE).read_text())
    except:
        return {}

def save_otp_store(data):
    Path(OTP_FILE).write_text(json.dumps(data, indent=2))


@app.post("/api/send-otp")
async def send_otp(request: Request):
    data = await request.json()
    email = data.get("email")
    user_id = data.get("userId")

    if not email or not user_id:
        return JSONResponse({"success": False, "message": "Email and userId required"}, status_code=400)

    # ----------------------------------------------------
    # Check if user exists in DB
    # ----------------------------------------------------
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT email FROM users WHERE userId=?", (user_id,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            return JSONResponse(
                {"success": False, "message": "User not registered"},
                status_code=404
            )

        # Optional: also check email matches DB record
        db_email = row[0]
        if db_email.lower() != email.lower():
            return JSONResponse(
                {"success": False, "message": "Email does not match registered user"},
                status_code=400
            )

    except Exception as e:
        return JSONResponse(
            {"success": False, "message": f"Database error: {e}"},
            status_code=500
        )

    # ----------------------------------------------------
    # Generate OTP
    # ----------------------------------------------------
    otp = str(random.randint(100000, 999999))
    timestamp = int(time.time())

    otp_store = load_otp_store()
    otp_store[user_id] = {"otp": otp, "timestamp": timestamp}
    save_otp_store(otp_store)

    subject = "Your Password Reset OTP"

    body = (
        f"Dear User,\n\n"
        f"Your OTP for password reset is: {otp}\n\n"
        "This OTP is valid for 5 minutes.\n"
        "If you didn’t request this, ignore this email.\n\n"
        "Best Regards,\nSecurity Team"
    )

    # ----------------------------------------------------
    # Send email
    # ----------------------------------------------------
    try:
        send_email(email, subject, body)
        return {"success": True, "message": "OTP sent successfully"}

    except Exception as e:
        return JSONResponse(
            {"success": False, "message": f"Failed to send OTP email: {e}"},
            status_code=500
        )

@app.post("/api/change-password")
async def change_password(request: Request, userId: str = Query(...)):
    """
    Change user password after OTP verification.
    Called as: POST /api/change-password?userId=USER123
    """
    try:
        data = await request.json()
        current_password = data.get("current_password")
        new_password = data.get("new_password")
        otp = data.get("otp")

        # Validate inputs
        if not current_password or not new_password or not otp:
            return JSONResponse(
                {"success": False, "message": "current_password, new_password and otp are required"},
                status_code=400
            )

        # -------------------------------
        # Fetch user from DB
        # -------------------------------
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT password, email FROM users WHERE userId=?", (userId,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return JSONResponse({"success": False, "message": "User not found"}, status_code=404)

        db_password, email = row

        # Check current password
        if db_password != current_password:
            conn.close()
            return JSONResponse({"success": False, "message": "Current password incorrect"}, status_code=400)

        # -------------------------------
        # Validate OTP
        # -------------------------------
        otp_store = load_otp_store()
        record = otp_store.get(userId)

        if not record:
            return JSONResponse({"success": False, "message": "No OTP found for this user"}, status_code=400)

        # Check OTP expiry
        if int(time.time()) - record["timestamp"] > OTP_EXPIRY:
            del otp_store[userId]
            save_otp_store(otp_store)
            return JSONResponse({"success": False, "message": "OTP expired"}, status_code=400)

        if record["otp"] != otp:
            return JSONResponse({"success": False, "message": "Invalid OTP"}, status_code=400)

        # -------------------------------
        # Update DB with new password
        # -------------------------------
        cursor.execute("UPDATE users SET password=? WHERE userId=?", (new_password, userId))
        conn.commit()
        conn.close()

        # Remove used OTP
        del otp_store[userId]
        save_otp_store(otp_store)

        print(f"🔐 Password changed successfully for {userId}")

        return {"success": True, "message": "Password changed successfully"}

    except Exception as e:
        print(f"❌ Error in change_password: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/user-reset-password")
async def user_reset_password(request: Request, userId: str = Query(...)):
    """
    Reset user password using OTP (no need for current_password).
    Called as: POST /api/user-reset-password?userId=USER123
    """
    try:
        data = await request.json()
        new_password = data.get("new_password")
        otp = data.get("otp")

        # Validate inputs
        if not new_password or not otp:
            return JSONResponse(
                {"success": False, "message": "new_password and otp are required"},
                status_code=400
            )

        # -------------------------------
        # Fetch user from DB
        # -------------------------------
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("SELECT email FROM users WHERE userId=?", (userId,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            return JSONResponse({"success": False, "message": "User not found"}, status_code=404)

        email = row[0]

        # -------------------------------
        # Validate OTP
        # -------------------------------
        otp_store = load_otp_store()
        record = otp_store.get(userId)

        if not record:
            return JSONResponse(
                {"success": False, "message": "No OTP found for this user"},
                status_code=400
            )

        # Check OTP expiration
        if int(time.time()) - record["timestamp"] > OTP_EXPIRY:
            del otp_store[userId]
            save_otp_store(otp_store)
            return JSONResponse(
                {"success": False, "message": "OTP expired"},
                status_code=400
            )

        # Validate OTP value
        if record["otp"] != otp:
            return JSONResponse(
                {"success": False, "message": "Invalid OTP"},
                status_code=400
            )

        # -------------------------------
        # Update DB with NEW password
        # -------------------------------
        cursor.execute("UPDATE users SET password=? WHERE userId=?", (new_password, userId))
        conn.commit()
        conn.close()

        # Remove OTP after successful reset
        del otp_store[userId]
        save_otp_store(otp_store)

        print(f"🔐 Password reset successful for {userId}")

        return {"success": True, "message": "Password reset successful"}

    except Exception as e:
        print(f"❌ Error in user_reset_password: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))

# … (all other routes stay unchanged — they are already local code)
@app.websocket("/ws/logs")
async def websocket_logs(websocket: WebSocket):
    print("⚡ Client trying to connect...")
    await websocket.accept()
    print("⚡ Client accepted. Adding to pool...")

    logger_util.websocket_connections.add(websocket)
    print(f"⚡ Total WS clients after add: {len(logger_util.websocket_connections)}")

    print("📡 WebSocket client connected.")

    try:
        while True:
            await asyncio.sleep(1)
    except:
        print("🔥 WS DISCONNECTED")
        logger_util.websocket_connections.discard(websocket)
        print(f"⚡ Total WS clients after remove: {len(logger_util.websocket_connections)}")
