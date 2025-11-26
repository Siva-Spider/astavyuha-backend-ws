# backend/logger_util.py
import os
import redis
import ssl
import json
from datetime import datetime
import datetime as _dt
from zoneinfo import ZoneInfo
import logging
from urllib.parse import urlparse

# Event loop is injected from main_fastapi on startup
event_loop = None

# ==========================================================
# 🌍 WebSocket Connection Pool (kept for bookkeeping only)
# ==========================================================
# Note: Direct WebSocket broadcasting from this module is removed.
# WebSocket handling should subscribe to Redis pub/sub channels in FastAPI.
websocket_connections = set()

# ==========================================================
# 🌐 Redis Setup
# ==========================================================
REDIS_URL = os.getenv("REDIS_URL", "").strip() or "redis://localhost:6379"

def connect_redis():
    try:
        parsed = urlparse(REDIS_URL)
        # Support rediss (TLS) if provided
        if parsed.scheme == "rediss":
            print(f"✅ Secure connection to Redis: {parsed.hostname}")
            return redis.StrictRedis.from_url(
                REDIS_URL,
                ssl_cert_reqs=ssl.CERT_NONE,
                decode_responses=True
            )
        else:
            print(f"✅ Connecting to Redis: {parsed.hostname}")
            return redis.StrictRedis.from_url(REDIS_URL, decode_responses=True)
    except Exception as e:
        print(f"⚠️ Redis connection failed: {e}")
        return None

redis_client = connect_redis()

# ==========================================================
# 🧠 Logger Setup (console)
# ==========================================================
console_logger = logging.getLogger("TradeLogger")
console_logger.setLevel(logging.INFO)
if not console_logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)-7s - %(message)s")
    handler.setFormatter(formatter)
    console_logger.addHandler(handler)

# ==========================================================
# Utility helpers
# ==========================================================
def _now_ts() -> str:
    """Return timestamp string in Asia/Kolkata timezone."""
    return datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")

def _date_str_from_ts(ts: str) -> str:
    return ts.split(" ")[0]

def _ensure_dir(path: str):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception as e:
        console_logger.warning(f"[logger_util] Could not create dir {path}: {e}")

# ==========================================================
# 📡 push_log - main logging API
# ==========================================================
def push_log(message: str, level: str = "info", user_id: str | None = None, log_type: str = "fastapi"):
    """
    Push a structured log entry.

    Args:
      - message: str message text
      - level: log level e.g. "info", "error", "warning"
      - user_id: optional user id; if provided logs are stored under that user
      - log_type: "trading" or "fastapi"
    """
    ts = _now_ts()
    entry = {
        "ts": ts,
        "level": level,
        "message": message,
        "type": log_type,
        "user_id": user_id
    }

    try:
        entry_json = json.dumps(entry, default=str)
    except Exception:
        # Fallback to simple string if serialization fails
        entry_json = json.dumps({
            "ts": ts,
            "level": level,
            "message": str(message),
            "type": log_type,
            "user_id": user_id
        })

    # Console output
    try:
        if level.lower() == "error":
            console_logger.error(entry_json)
        elif level.lower() == "warning" or level.lower() == "warn":
            console_logger.warning(entry_json)
        else:
            console_logger.info(entry_json)
    except Exception:
        # Swallow console logging errors
        pass

    # ---------- 1) Persist to daily file per category ----------
    date_str = _date_str_from_ts(ts)

    try:
        if log_type == "trading":
            # trading logs must have user_id set (but we guard)
            uid = user_id or "system"
            folder = os.path.join("logs", "trading", uid)
        elif user_id:
            # FastAPI logs tied to a user
            folder = os.path.join("logs", "fastapi", "users", user_id)
        else:
            # system-level FastAPI logs
            folder = os.path.join("logs", "fastapi", "system")

        _ensure_dir(folder)
        file_path = os.path.join(folder, f"{date_str}.json")

        with open(file_path, "a", encoding="utf-8") as f:
            f.write(entry_json + "\n")
    except Exception as e:
        console_logger.warning(f"[push_log] failed to write file log: {e}")

    # ---------- 2) Push to Redis (list + pubsub) ----------
    if not redis_client:
        return

    try:
        if log_type == "trading":
            uid = user_id or "system"
            redis_client.rpush(f"autotrade_logs:{uid}", entry_json)
            redis_client.publish(f"log_stream:{uid}", entry_json)

        elif user_id:
            redis_client.rpush(f"fastapi_logs:{user_id}", entry_json)
            redis_client.publish(f"fastapi_stream:{user_id}", entry_json)

        else:
            # system fastapi logs
            redis_client.rpush("fastapi_logs:system", entry_json)
            redis_client.publish("fastapi_stream:system", entry_json)

    except Exception as e:
        console_logger.warning(f"[push_log] Redis unavailable: {e}")

# ==========================================================
# 📤 push_payload - data/payload dumps for a user (or system)
# ==========================================================
def push_payload(name: str, data, user_id: str | None = None):
    """
    Store a payload dump (e.g. instrument payloads, candle dumps).
    Stored in logs/payload/{user_id}/{date}.json and pushed to autotrade_logs:{user_id}.
    """
    ts = _now_ts()
    entry = {
        "type": "payload",
        "ts": ts,
        "name": name,
        "data": data,
        "user_id": user_id
    }

    try:
        entry_json = json.dumps(entry, default=str)
    except Exception:
        entry_json = json.dumps({
            "type": "payload",
            "ts": ts,
            "name": name,
            "data": str(data),
            "user_id": user_id
        })

    # file storage
    date_str = _date_str_from_ts(ts)
    try:
        if user_id:
            folder = os.path.join("logs", "payload", user_id)
        else:
            folder = os.path.join("logs", "payload", "system")
        _ensure_dir(folder)
        file_path = os.path.join(folder, f"{date_str}.json")
        with open(file_path, "a", encoding="utf-8") as f:
            f.write(entry_json + "\n")
    except Exception as e:
        console_logger.warning(f"[push_payload] failed to write file: {e}")

    # redis
    if not redis_client:
        return
    try:
        uid = user_id or "system"
        redis_client.rpush(f"autotrade_logs:{uid}", entry_json)
        redis_client.publish(f"log_stream:{uid}", entry_json)
    except Exception:
        # best-effort push; do not crash execution
        pass

# ==========================================================
# Optional helpers for admin or debugging
# ==========================================================
def list_log_files(log_type: str = "trading", user_id: str | None = None, days: int = 30):
    """
    Returns a list of log file paths for the requested type & user for the last `days`.
    This does not read the file contents - it only returns candidate filenames (most recent first).
    """
    files = []
    try:
        if log_type == "trading":
            base = os.path.join("logs", "trading", user_id or "")
        elif log_type == "fastapi":
            base = os.path.join("logs", "fastapi", "users", user_id) if user_id else os.path.join("logs", "fastapi", "system")
        else:
            return files

        if not os.path.exists(base):
            return files

        for i in range(days + 1):
            d = (_dt.datetime.now() - _dt.timedelta(days=i)).strftime("%Y-%m-%d")
            candidate = os.path.join(base, f"{d}.json")
            if os.path.exists(candidate):
                files.append(candidate)
    except Exception:
        pass
    return files

def read_log_file(path: str, max_lines: int = 1000):
    """
    Read lines from the given log file path and return as list of parsed JSON entries (up to max_lines).
    """
    entries = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= max_lines:
                    break
                try:
                    entries.append(json.loads(line.strip()))
                except Exception:
                    entries.append({"raw": line.strip()})
    except Exception:
        pass
    return entries


def fastapi_log(message: str, user_id=None, level="info"):
    print(message)  # terminal print stays

    # Now store the same message as fastapi log
    push_log(
        message=message,
        level=level,
        user_id=user_id,
        log_type="fastapi"
    )


# ==========================================================
# Public API export
# ==========================================================
__all__ = ["push_log", "push_payload", "websocket_connections", "list_log_files", "read_log_file", "connect_redis", "fastapi_log"]
