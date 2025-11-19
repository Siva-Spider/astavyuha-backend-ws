import sys
import os

# Ensure project root is in PYTHONPATH
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))   # backend/
PROJECT_ROOT = os.path.dirname(ROOT_DIR)                # project5/
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from celery import Celery

try:
    CELERY_BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/1")
    CELERY_RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/2")

    import redis
    r = redis.Redis(host="localhost", port=6379, db=1)
    r.ping()
    print("✅ Connected to real Redis")

except Exception:
    print("⚠️ Redis not found, using in-memory FakeRedis for Celery")
    import fakeredis

    CELERY_BROKER_URL = "memory://"
    CELERY_RESULT_BACKEND = "cache+memory://"

celery_app = Celery(
    "astavyuha_tasks",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND
)

celery_app.conf.update(
    task_track_started=True,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Asia/Kolkata",
    enable_utc=False
)

# 🔥 Force Celery to import your trading tasks
celery_app.conf.imports = ("backend.tasks.trading_tasks",)

# THE MOST IMPORTANT LINE
celery_app.autodiscover_tasks(packages=["backend.tasks"])
