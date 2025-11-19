# main_fastapi.py
from fastapi import FastAPI, HTTPException, Depends, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi import WebSocket, WebSocketDisconnect
import backend.logger_util as logger_util
# from backend.logger_util import websocket_connections, logger_util.push_log, get_log_buffer
from fastapi.middleware.wsgi import WSGIMiddleware
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

import threading, json, asyncio
from queue import Queue, Empty
from backend import Upstox as us

# Import helpers used in many endpoints
from backend.update_db import init_db
from backend.password_utils import generate_random_password
from backend.email_utils import send_email

from fastapi import Request
import queue
import redis, json, time, asyncio, threading, queue
import ssl
from urllib.parse import urlparse

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
            logger_util.push_log(f"⚠️ Login attempt failed: user {data.userId} not found")
            return {"success": False, "message": "Invalid User ID"}

        db_userId, db_password, db_role, db_email, db_mobile, db_username = row

        if db_password != data.password:
            logger_util.push_log(f"⚠️ Incorrect password for user {data.userId}")
            return {"success": False, "message": "Incorrect password"}

        if data.role:
            normalized_role = data.role.lower().strip()
            db_normalized_role = db_role.lower().strip()
            if normalized_role == "client":
                normalized_role = "user"
            if normalized_role != db_normalized_role:
                logger_util.push_log(f"⚠️ Role mismatch: provided {data.role}, db {db_role}")
                return {"success": False, "message": "Role mismatch"}

        profile = {
            "userid": db_userId,
            "username": db_username or db_userId,
            "email": db_email or "",
            "mobilenumber": db_mobile or "",
            "role": db_role,
        }
        logger_util.push_log(f"✅ User {data.userId} logged in as {db_role}")
        return {"success": True, "message": "Login successful", "profile": profile}

    except Exception as e:
        logger_util.push_log(f"❌ Login error: {e}", "error")
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

        password = generate_random_password()

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
            logger_util.push_log(f"⚠️ Pending user email failed for {data.email}: {mail_err}")

        logger_util.push_log(f"🕓 New pending registration: {user_id_candidate} ({role})")

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
        logger_util.push_log(f"❌ Error in pending registration: {e}", "error")
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

        logger_util.push_log(f"✅ Profit/Loss fetched successfully for {segment} FY{fy_code}")

        return JSONResponse(content={"success": True, "data": result, "rows": charges})

    except Exception as e:
        try:
            logger_util.push_log(f"❌ Error in /api/get_profit_loss: {e}", "error")
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
        logger_util.push_log("💾 Saved unified trading configuration to trading_config.json")

        task = start_trading_loop.apply_async()
        logger_util.push_log(f"🟢 Celery task {task.id} started.")

        return {"success": True, "message": "Trading loop started.", "task_id": task.id}

    except Exception as e:
        logger_util.push_log(f"❌ Could not start trading task: {e}", "error")
        return {"success": False, "message": str(e)}


# … (all other routes stay unchanged — they are already local code)

@app.websocket("/ws/logs")
async def websocket_logs(websocket: WebSocket):
    print("⚡ Client trying to connect...")
    await websocket.accept()
    print("⚡ Client accepted. Adding to pool...")

    logger_util.websocket_connections.add(websocket)
    print(f"⚡ Total WS clients after add: {len(logger_util.websocket_connections)}")

    logger_util.push_log("📡 WebSocket client connected.")

    try:
        while True:
            await asyncio.sleep(1)
    except:
        print("🔥 WS DISCONNECTED")
        logger_util.websocket_connections.discard(websocket)
        print(f"⚡ Total WS clients after remove: {len(logger_util.websocket_connections)}")
