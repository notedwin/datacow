import json
import threading
import time

import requests
import schedule
from flask import Flask, g, render_template, request
from peewee import (
    CharField,
    DateTimeField,
    IntegerField,
    Model,
    SqliteDatabase,
    TextField,
    fn,
)
from playhouse.shortcuts import model_to_dict

DATABASE = "data.db"
DEBUG = True
SECRET_KEY = "hin6bab8ge25*r=x&amp;+5$0kn=-#log$pt^#@vrqjld!^2ci@g*b"


database = SqliteDatabase(DATABASE, check_same_thread=False)
app = Flask(__name__)
app.config.from_object(__name__)


class BaseModel(Model):
    class Meta:
        database = database


class Event(BaseModel):
    name = CharField(unique=True)
    message = TextField()

    interval_minutes = IntegerField()
    reminder_minutes = IntegerField()
    last_completed = DateTimeField(null=True)
    last_reminded = DateTimeField(null=True)


class EventHistory(BaseModel):
    event_id = IntegerField(null=True, unique=True)
    event = CharField()


@app.before_request
def before_request():
    g.db = database
    g.db.connect()


@app.after_request
def after_request(response):
    g.db.close()
    return response


@app.route("/", methods=["GET", "POST"])
def home():
    if request.method == "POST":
        e = Event.create(
            name=request.form["name"],
            message=request.form["message"],
            interval_minutes=request.form["interval_minutes"],
            reminder_minutes=request.form["reminder_minutes"],
        )
        return json.dumps(model_to_dict(e))

    return render_template("/create.html")


@app.get("/log/<event_name>")
def log_event(event_name):
    try:
        e = Event.get(Event.name == event_name)
        e.last_completed = fn.unixepoch()
        e.save()

        return json.dumps(model_to_dict(e), default=str)

    except Event.DoesNotExist:
        print("No event?")
        return {}


@app.get("/notify")
def notify_event():
    check_and_send_notifications()
    return {}


def send_notif(event):
    print(f"Sending push notification for event: {event.name}")
    requests.post(
        "http://notedwin:82/edwin",
        data=f"{event.message}, reminder in {event.reminder_minutes} minutes",
        headers={
            "Title": event.name,
            "Icon": "https://styles.redditmedia.com/t5_32uhe/styles/communityIcon_xnt6chtnr2j21.png",
            "Priority": "urgent",
            "Tags": "warning,skull",
        },
    )


def check_and_send_notifications():
    event_due = (
        # if we havent sent a reminder, check time from last_completed
        # if we have sent reminder, check time from last reminded
        (Event.last_reminded.is_null()) & Event.interval_minutes
        < ((fn.unixepoch() - Event.last_completed) / 60) | Event.reminder_minutes
        < ((fn.unixepoch() - Event.last_reminded) / 60)
    )

    events = Event.select().where(event_due)
    for event in events:
        send_notif(event)
        event.last_reminded = fn.unixepoch()
        event.save()


# # Schedule the job to run every minute
schedule.every().minute.at(":23").do(check_and_send_notifications)


# # Run the scheduler in a separate thread
def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.start()
    with database:
        database.create_tables([Event, EventHistory])
    app.run()
