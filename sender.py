import requests
from time import sleep
import sqlite3
from datetime import datetime
import geopy.distance


TOKEN = ""
CHAT_ID = ""


def main():
    while True:
        message = create_message()
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={CHAT_ID}&text={message}"
        requests.get(url).json()
        sleep(10)


def create_message():
    message = ""
    events = compare_events()
    for event in events:
        message += f"Your planned event {event['calendar_event']} at " \
                   f"{event['event_datetime']} is threatened by {event['disaster']}.\n\n"

    return message


def compare_events():
    disasters = read_disasters()
    calendar = read_calendar()

    for c in calendar:
        for d in disasters:
            disaster_longitude = d[2]
            disaster_latitude = d[3]
            disaster_coords = (disaster_latitude, disaster_longitude, )

            calendar_longitude = c[2]
            calendar_latitude = c[3]
            calendar_coords = (calendar_latitude / 2, calendar_longitude)

            distance = geopy.distance.geodesic(disaster_coords, calendar_coords).km
            if distance <= 300:

                disaster_time = datetime.strptime(d[4], "%Y-%m-%dT%H:%M:%SZ")
                calendar_time = datetime.strptime(c[4][:-6], "%Y-%m-%dT%H:%M:%S")
                event_delta = abs((calendar_time - disaster_time).days)
                if event_delta < 45:
                    yield {"calendar_event": c[1], "event_datetime": c[4], "disaster": d[1]}


def read_disasters():
    con = sqlite3.connect("disaster.db")
    cur = con.cursor()

    disasters = cur.execute("SELECT id, title, longitude, latitude, event_datetime FROM disaster_event").fetchall()

    cur.close()
    con.close()

    return disasters


def read_calendar():
    con = sqlite3.connect("disaster.db")
    cur = con.cursor()

    disasters = cur.execute("SELECT id, title, longitude, latitude, event_datetime FROM calendar_event").fetchall()

    cur.close()
    con.close()

    return disasters


if __name__ == "__main__":
    main()




