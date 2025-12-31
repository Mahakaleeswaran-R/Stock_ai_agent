import asyncio
import json
import logging
import os
import functools
from datetime import timedelta

import firebase_admin
from firebase_admin import credentials, messaging, exceptions

from config import redis_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("FCM_PUBLISHER")

try:
    if not firebase_admin._apps:
        if os.path.exists("/etc/secrets/service-account.json"):
            cred_path = "/etc/secrets/service-account.json"
            logger.info("Loading Firebase credentials from Render secrets")
        elif os.path.exists("./service-account.json"):
            cred_path = "./service-account.json"
            logger.info("Loading Firebase credentials from local file")
        else:
            cred_json = os.environ.get("FIREBASE_CREDENTIALS_JSON")
            if cred_json:
                logger.info("Loading Firebase credentials from Environment Variable")
                cred_dict = json.loads(cred_json)
                cred = credentials.Certificate(cred_dict)
            else:
                raise FileNotFoundError("No service-account.json found!")

        if 'cred' not in locals():
            cred = credentials.Certificate(cred_path)

        firebase_admin.initialize_app(cred)
        logger.info("Firebase Admin Initialized")
except ValueError:
    pass
except Exception as e:
    logger.error(f"Firebase Init Failed: {e}")
    raise

TOPIC_NAME = "trades"
FCM_TTL_SECONDS = 300  # 5 Minutes TTL (News expires fast)


async def send_notification(signal: dict) -> bool:
    try:
        # 1. Parse Core Data
        symbol = signal.get("symbol", "UNKNOWN").replace(".NS", "").replace(".BO", "")
        side = signal.get("direction", "ALERT")  # BUY/SELL
        trade_id = signal.get("trade_id", "")

        # 2. Parse Trade Idea
        idea = signal.get("trade_idea", {})
        entry_price = idea.get("entry", 0)
        stop_loss = idea.get("stop_loss", 0)
        qty = idea.get("quantity", 0)
        targets = idea.get("reference_targets", {})
        t1, t2, t3 = targets.get("t1", 0), targets.get("t2", 0), targets.get("t3", 0)

        # 3. Parse AI & Reason
        catalyst = signal.get("reason", "Technical Breakout")
        ai_conf = signal.get("ai_confidence", 0)
        tech_conf = signal.get("tech_confidence", 0)

        # 4. Parse Summary
        pdf_points = signal.get("event_summary", [])
        if not pdf_points:
            pdf_points = signal.get("ai_summary", {}).get("pdf_summary", [])

        # Format Summary (Max 3 points)
        summary_text = ""
        if pdf_points:
            clean_points = [p.lstrip("- ").strip() for p in pdf_points[:3]]
            summary_text = "📌 " + "\n📌 ".join(clean_points)
        else:
            summary_text = "⚠️ No detailed event summary available."

        # 5. Construct Notification
        emoji = "🚀" if side == "BUY" else "🔻"
        notif_title = f"{emoji} {side} {symbol} @ {entry_price}"

        # Calculate R:R for display
        risk = abs(entry_price - stop_loss)
        reward = abs(t1 - entry_price)
        rr_ratio = round(reward / risk, 1) if risk > 0 else 0

        notif_body = (
            f"💡 Catalyst: {catalyst}\n"
            f"🤖 AI Conf: {int(ai_conf * 100)}% | Tech Conf: {int(tech_conf * 100)}%\n\n"
            f"🎯 Targets: {t1} | {t2} | {t3}\n"
            f"🛑 Stop: {stop_loss} (Risk: {rr_ratio}R)\n"
            f"📦 Qty: {qty}\n\n"
            f"{summary_text}"
        )

        # 6. Data Payload (For App Logic)
        data_payload = {
            "click_action": "FLUTTER_NOTIFICATION_CLICK",
            "type": "TRADE_SIGNAL",
            "trade_id": str(trade_id),
            "symbol": symbol,
            "side": side,
            "entry": str(entry_price),
            "stop_loss": str(stop_loss),
            "t1": str(t1),
            "t2": str(t2),
            "t3": str(t3),
            "qty": str(qty),
            "catalyst": str(catalyst),
            "ai_conf": str(ai_conf),
            "timestamp": str(signal.get("timestamp", ""))
        }

        # 7. Send
        message = messaging.Message(
            notification=messaging.Notification(
                title=notif_title,
                body=notif_body
            ),
            data=data_payload,
            topic=TOPIC_NAME,
            android=messaging.AndroidConfig(
                priority="high",
                ttl=timedelta(seconds=FCM_TTL_SECONDS),
                notification=messaging.AndroidNotification(
                    channel_id="trade_alerts_v1",
                    icon="stock_icon",
                    color="#00C853" if side == "BUY" else "#D50000"
                )
            )
        )

        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(
            None, functools.partial(messaging.send, message)
        )

        logger.info(f"FCM SENT | {symbol} {side} | ID: {response}")
        return True

    except exceptions.FirebaseError as e:
        # Handle Quota Limits safely
        msg = str(e).lower()
        if "quota" in msg or "429" in msg:
            logger.warning("FCM Quota Exceeded. Pausing 15s...")
            await asyncio.sleep(15)
        else:
            logger.error(f"Firebase Error: {e}")
        return False

    except Exception as e:
        logger.error(f"Notification Logic Error: {e}", exc_info=True)
        return False  # Return False so we don't lose the signal (Re-queue it)


async def run():
    logger.info("FCM Publisher Active & Listening on QUEUE:TRADE_SIGNALS...")
    while True:
        try:
            # Blocking Pop from Redis
            item = await redis_client.blpop("QUEUE:TRADE_SIGNALS", timeout=60)

            if not item:
                await asyncio.sleep(0.1)
                continue

            raw_data = item[1]
            signal_data = json.loads(raw_data)

            # Attempt Send
            success = await send_notification(signal_data)

            # Retry Logic
            if not success:
                retry_count = signal_data.get("_retry_count", 0)
                if retry_count < 3:
                    logger.warning(
                        f"Failed to send {signal_data.get('symbol')}. Re-queuing (Attempt {retry_count + 1}/3)")
                    signal_data["_retry_count"] = retry_count + 1
                    await asyncio.sleep(2)
                    # Push back to RIGHT of queue (so we don't block new signals)
                    await redis_client.rpush("QUEUE:TRADE_SIGNALS", json.dumps(signal_data))
                else:
                    logger.error(f"Dropping Signal {signal_data.get('symbol')} after 3 failed FCM attempts.")

        except Exception as e:
            logger.error(f"Main Loop Crash: {e}", exc_info=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("FCM Publisher Stopped")