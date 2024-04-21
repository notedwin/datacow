# a continouus running process that polls for files in a s3 bucket, use duckdb for metadata
# and then use the metadata to generate a report
import logging
import time
from datetime import datetime
from functools import wraps

import cal
import cf_export
import requests
import schedule
import structlog

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    processors=[
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.add_log_level,
        structlog.processors.UnicodeDecoder(),
        structlog.dev.ConsoleRenderer(colors=True),
    ],
)


log = structlog.get_logger()
URL = "http://spark:8002/ping/28c66223-4d86-497d-b1de-c83c6f6c56fe"


def task(func):
    @wraps(func)
    def task_closure(*args, **kwargs):
        start = datetime.now()
        log.info(f"Function: {func.__name__} started at: {start}")
        try:
            requests.get(URL + "/start", timeout=3)
            result = func(*args, **kwargs)
            # with capture_logs() as cap_logs:
            #     result = func(*args, **kwargs)
            # logs = ""
            # for e in cap_logs:
            #     logs += e["log_level"] + " " + e["event"]
            #     for k, v in e["args"].items():
            #         logs += f" {k}: {v}"
            #     logs += "\n"
            # requests.post(URL, data=cap_logs)
        except Exception as e:
            log.exception(
                f"Function: {func.__name__} failed with error: {e}", stack_info=True
            )
            requests.get(URL + "/fail")
            return None
        end = datetime.now()
        log.info(f"Function: {func.__name__} executed in: {end - start}")
        return result

    return task_closure


@task
def heart_beat():
    log.info("Heartbeat")


schedule.every(6).hours.do(heart_beat)
# every 2 days
schedule.every(48).hours.do(task(cal.main))
schedule.every(48).hours.do(task(cf_export.main))
# add an event to build new reports

# run once when the script is started
task(cal.main)()
task(cf_export.main)()

while True:
    schedule.run_pending()
    time.sleep(600)
