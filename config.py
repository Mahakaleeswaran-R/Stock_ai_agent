import os
from redis.asyncio import Redis
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017/")

redis_client = Redis.from_url(REDIS_URL, decode_responses=True)

mongo_client = AsyncIOMotorClient(MONGO_URL)
db = mongo_client["stock_agent"]

# Collections
raw_events = db["raw_events"]
trade_signals = db["trade_signals"]
ai_audit = db["ai_analysis"]
technical_audit = db["technical_analysis"]
system_performance = db["system_performance"]

# --- Credentials ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

ANGEL_API_KEY = os.getenv("ANGEL_API_KEY")
ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID")
ANGEL_PIN = os.getenv("ANGEL_PIN")
ANGEL_TOTP_KEY = os.getenv("ANGEL_TOTP_KEY")

# Validation check to ensure keys are loaded
if not GEMINI_API_KEY:
    print("WARNING: GEMINI_API_KEY is missing!")
if not ANGEL_API_KEY:
    print("WARNING: ANGEL_API_KEY is missing!")