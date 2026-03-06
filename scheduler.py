import logging
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from database import SessionLocal
from infoStreamDigest import InfoStreamDigest
from models import UserLocation, NewsTopicAndScheduleTime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler()


def run_digest_for_slot(delivery_time: str, timezone_str: str):
    """Runs for a specific delivery_time + timezone combination."""
    db = SessionLocal()
    try:
        digest = InfoStreamDigest()
        result = digest.send_emails_batch(
            db,
            target_time=delivery_time,
            target_timezone=timezone_str  # ← you'll add this filter to infoStreamDigest.py
        )
        logger.info(f"[{timezone_str}] {delivery_time} job result: {result}")
    except Exception as e:
        logger.error(f"Error in digest job [{timezone_str} {delivery_time}]: {str(e)}")
    finally:
        db.close()

def check_immediate_emails():
    db = SessionLocal()
    try:
        digest = InfoStreamDigest()
        result = digest.send_immediate_email(db)
        logger.info(f'Immediate digest job: {result}')
    except Exception as e:
        logger.error(f"Error checking immediate emails: {str(e)}")
    finally:
        db.close()


# Time string "11:00 AM" → (hour=11, minute=0)
# Time string "02:00 PM" → (hour=14, minute=0)
def parse_delivery_time(delivery_time: str):
    """Convert '11:00 AM' / '02:00 PM' to 24h hour and minute integers."""
    from datetime import datetime
    dt = datetime.strptime(delivery_time, '%I:%M %p')
    return dt.hour, dt.minute


def load_and_schedule_jobs():
    """
    Reads distinct timezone + delivery_time combos from DB.
    Creates one cron job per unique combination.
    Called on startup AND whenever a user adds/changes their preference.
    """
    db = SessionLocal()
    try:
        # Get all unique (timezone, deliveryTime) pairs — no matter how many users
        slots = db.query(
            UserLocation.timezone_,
            NewsTopicAndScheduleTime.deliveryTime
        ).join(
            NewsTopicAndScheduleTime,
            UserLocation.user_id == NewsTopicAndScheduleTime.user_id
        ).filter(
            NewsTopicAndScheduleTime.isScheduled == True
        ).distinct().all()

        if not slots:
            logger.info("No scheduled preferences found in DB")
            return

        # Remove old scheduled jobs (not immediate check)
        for job in scheduler.get_jobs():
            if job.id.startswith('digest_'):
                job.remove()

        # Add one job per unique slot
        for timezone_str, delivery_time in slots:
            try:
                tz = pytz.timezone(timezone_str or 'UTC')
                hour, minute = parse_delivery_time(delivery_time)

                job_id = f"digest_{timezone_str}_{delivery_time}".replace(' ', '_').replace('/', '_')

                scheduler.add_job(
                    lambda dt=delivery_time, tz_str=timezone_str: run_digest_for_slot(dt, tz_str),
                    CronTrigger(hour=hour, minute=minute, timezone=tz),
                    id=job_id,
                    name=f"{delivery_time} ({timezone_str})",
                    replace_existing=True
                )
                logger.info(f"  ✓ Scheduled: {delivery_time} @ {timezone_str}")

            except Exception as e:
                logger.error(f"Failed to schedule slot [{timezone_str} {delivery_time}]: {e}")

        logger.info(f"Total digest jobs scheduled: {len([j for j in scheduler.get_jobs() if j.id.startswith('digest_')])}")

    finally:
        db.close()


def start_scheduler():
    # Immediate emails — still runs every minute (lightweight, just checks a flag)
    scheduler.add_job(
        check_immediate_emails,
        'interval',
        minutes=1,
        id='check_immediate',
        name='Check Immediate Emails',
        replace_existing=True
    )

    # Load dynamic jobs from DB
    load_and_schedule_jobs()

    scheduler.start()
    logger.info("✓ Scheduler started")


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown()
        logger.info("Scheduler stopped gracefully")