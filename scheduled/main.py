import json
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

API_KEY = "VpQ59KiLeHVGdg8zYDzE-9hyGfOvq5Ca"  # only works from authorized IP
log = structlog.get_logger()
URL = "http://192.168.0.200:8002/api/v3/checks/"

# todo set default timeout for requests, use exponential backoff for retries
# use library from same guy as structlog


class Job:
    def __init__(self, id, name, desc, interval, fn):
        self.id = id
        self.name = name
        self.desc = desc
        self.interval = interval
        self.fn = fn

        self.channels = "*"

    def get_json(self):
        # json sent to health checks
        return json.dumps(
            {
                "name": self.name,
                "slug": self.name,
                "desc": self.desc,
                "timeout": self.interval,
                "grace_period": 60 * 60,
                "channels": self.channels,
            }
        )


def heart_beat():
    log.info("Heartbeat")


def hours_to_seconds(hours):
    return hours * 60 * 60


def task(func, hc_url):
    @wraps(func)
    def task_closure(*args, **kwargs):
        start = datetime.now()
        log.info(f"Function: {func.__name__} started at: {start}")
        try:
            requests.get(hc_url + "/start", timeout=3)
            result = func(*args, **kwargs)
        except Exception as e:
            log.exception(
                f"Function: {func.__name__} failed with error: {e}", stack_info=True
            )
            requests.get(hc_url + "/fail")
            return None
        requests.get(hc_url, timeout=3)
        end = datetime.now()
        log.info(f"Function: {func.__name__} executed in: {end - start}")
        return result

    return task_closure


def delete_all_jobs():
    r = requests.get(URL, headers={"X-Api-Key": API_KEY}).json()

    for job in r["checks"]:
        job = job["update_url"].split("/")[-1]

        r = requests.delete(
            f"http://spark:8002/api/v3/checks/{job}", headers={"X-Api-Key": API_KEY}
        )
    log.info("Deleted all jobs")


def create_or_update_job(jobs):
    """Creates or updates job in health checks,
    schedules job and runs it once
    """
    name = jobs.name
    json = jobs.get_json()

    created = requests.post(URL, headers={"X-Api-Key": API_KEY}, data=json)

    if created.status_code not in [200, 201]:
        log.error(f"Failed to create {name}")
        return

    log.info(f"Created {name}")

    health = requests.get(URL + "?slug=" + name, headers={"X-Api-Key": API_KEY})

    ping_url = health.json()["checks"][0]["ping_url"]

    fn = task(jobs.fn, ping_url)

    schedule.every(jobs.interval).seconds.do(fn)
    fn()


if __name__ == "__main__":
    jobs = [
        Job(
            id=1,
            name="heart_beat",
            desc="heart_beat for scheduler",
            interval=hours_to_seconds(6),
            fn=heart_beat,
        ),
        Job(
            id=2,
            name="cal",
            desc="Generate contribution calendar",
            interval=hours_to_seconds(48),
            fn=cal.main,
        ),
        Job(
            id=3,
            name="cf_export",
            desc="Export cloudflare data",
            interval=hours_to_seconds(48),
            fn=cf_export.main,
        ),
    ]

    delete_all_jobs()
    for job in jobs:
        create_or_update_job(job)

    while True:
        n = schedule.idle_seconds()
        if n is None:
            # no more jobs
            break
        elif n > 0:
            # sleep exactly the right amount of time
            log.info(f"Next job in {n} seconds")
            time.sleep(n)
        schedule.run_pending()
