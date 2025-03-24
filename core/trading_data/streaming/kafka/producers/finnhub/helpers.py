from datetime import datetime, timedelta
import time

def sleep_until_next_minute_plus_10():
    now = datetime.now()
    next_minute = now + timedelta(minutes=1)
    next_minute = next_minute.replace(second=10, microsecond=0)
    sleep_time = (next_minute - now).total_seconds()
    time.sleep(sleep_time)

def is_within_work_hours():
  now = datetime.now()
  weekday = now.weekday()  # 0: Monday, 1: Tuesday, ..., 4: Friday
  hour = now.hour
  minute = now.minute

  return weekday < 5 and (7 <= hour <= 12 or (hour == 6 and minute >=30) or (hour == 13 and minute == 0))