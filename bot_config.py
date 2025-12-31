import json
import logging
from dataclasses import dataclass, asdict

from config import redis_client

logger = logging.getLogger("SYSTEM_CONFIG")


@dataclass
class BotConfig:
    # --- AI ENGINE PARAMS ---
    AI_MIN_CONFIDENCE: float = 0.65  # Minimum confidence to consider a signal
    AI_TIER_EXTREME_BOOST: float = 0.05  # How much confidence boost for EXTREME tier

    # --- TECHNICAL ENGINE PARAMS ---
    RISK_PER_TRADE_PCT: float = 0.0025  # 0.25% of capital per trade
    MAX_POSITION_SIZE_PCT: float = 0.10  # Max 10% of capital in one stock
    MAX_DAILY_LOSS_PCT: float = -0.015  # Stop trading if down 1.5% today
    MAX_CONCURRENT_TRADES: int = 5  # Max active positions

    # --- EXECUTION PARAMS ---
    SLIPPAGE_TOLERANCE: float = 0.005  # 0.5% slippage allowed

    @classmethod
    def get_defaults(cls):
        return cls()

    @classmethod
    async def load(cls):
        try:
            data = await redis_client.get("CONFIG:DYNAMIC_SETTINGS")
            if data:
                config_dict = json.loads(data)
                default_dict = asdict(cls())
                merged_config = {k: config_dict.get(k, v) for k, v in default_dict.items()}
                return cls(**merged_config)
        except Exception as e:
            logger.error(f"Config Load Failed (Using Defaults): {e}")

        return cls.get_defaults()

    @classmethod
    async def save(cls, config_obj):
        try:
            data = json.dumps(asdict(config_obj))
            await redis_client.set("CONFIG:DYNAMIC_SETTINGS", data)
            logger.info("System Configuration Updated in Redis")
        except Exception as e:
            logger.error(f"Failed to save config: {e}")