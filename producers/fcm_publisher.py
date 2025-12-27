import asyncio
import json
import logging
import os
import firebase_admin
from firebase_admin import credentials, messaging, exceptions
from config import redis_client
import functools

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("FCM_PUBLISHER")

try:
    if os.path.exists("/etc/secrets/service-account.json"):
        cred_path = "/etc/secrets/service-account.json"
        logger.info("Loading Firebase Creds from Render Secret File...")
    else:
        cred_path = "./service-account.json"
        logger.info("Loading Firebase Creds from Local File...")
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)
    logger.info("Firebase Admin Initialized")
except ValueError:
    pass
except Exception as e:
    logger.error(f"Firebase Init Failed: {e}")

TOPIC_NAME = "trades"


async def send_notification(signal):
    try:
        symbol = signal.get('symbol', 'UNKNOWN').replace('.NS', '').replace('.BO', '')
        side = signal.get('signal', 'ALERT')
        entry = signal.get('entry', 0)

        ai_data = signal.get('ai_analysis', {})
        catalyst = ai_data.get('catalyst', 'Technical Signal')
        tier = ai_data.get('tier', 'MODERATE')

        emoji = "🐂" if side == "BUY" else "🐻"
        notif_title = f"{emoji} {catalyst}"
        notif_body = f"{tier} {side}: {symbol} @ ₹{entry}"

        data_payload = {
            "click_action": "FLUTTER_NOTIFICATION_CLICK",
            "symbol": str(symbol),
            "signal": str(side),
            "entry": str(entry),
            "stop_loss": str(signal.get('stop_loss', '')),
            "take_profit": str(signal.get('take_profit', '')),
            "tier": str(tier),
            "reason": str(catalyst),
            "rsi": str(signal.get('instructions', {}).get('rsi', '')),
            "status": str(signal.get('instructions', {}).get('status', '')),
            "timestamp": str(signal.get('timestamp', '')),
            "full_json": json.dumps(signal)
        }

        message = messaging.Message(
            notification=messaging.Notification(
                title=notif_title,
                body=notif_body,
            ),
            data=data_payload,
            topic=TOPIC_NAME,
            android=messaging.AndroidConfig(priority='high')
        )
        
        loop = asyncio.get_running_loop()
        send_func = functools.partial(
            messaging.send, 
            message
        )
        response = await loop.run_in_executor(None, send_func)
        logger.info(f"Sent: {symbol} ({side}) | ID: {response}")
        return True

    except exceptions.FirebaseError as e:
        if "quota" in str(e).lower() or "429" in str(e):
            logger.warning(f"Quota Exceeded! Sleeping 15s... Error: {e}")
            await asyncio.sleep(15)
        else:
            logger.error(f"FCM Send Error: {e}")
        return False
    except Exception as e:
        logger.error(f"General Error: {e}")
        return False


async def run():
    logger.info("FCM Publisher Service Active & Listening...")
    while True:
        try:
            item = await redis_client.blpop("QUEUE:TRADE_SIGNALS", timeout=60)

            if item:
                raw_data = item[1]
                signal_data = json.loads(raw_data)
                await send_notification(signal_data)
            else:
                await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"Loop Error: {e}")
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("FCM Publisher Stopped.")
