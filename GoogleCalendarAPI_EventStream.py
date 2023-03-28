from __future__ import print_function

import datetime
import os.path
import datetime

import asyncio
import nest_asyncio
import reactivex
import disaster_saver

from reactivex import operators as ops, Observable
from functools import wraps, partial

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/calendar']

"""Shows basic usage of the Google Calendar API.
Prints the start and name of the next 10 events on the user's calendar.
"""
creds = None
# The file token.json stores the user's access and refresh tokens, and is
# created automatically when the authorization flow completes for the first
# time.

def create_creds():
    global creds
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        return creds
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
            return creds
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())



nest_asyncio.apply()

def wrap(func):
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)
    return run


@wrap
def send_request():

    try:
        service = build('calendar', 'v3', credentials=create_creds())

        # Call the Calendar API
        now = datetime.datetime.utcnow().isoformat() + 'Z'  # 'Z' indicates UTC time
        events_result = service.events().list(calendarId='primary', timeMax=now,
                                              maxResults=1000, singleEvents=True,
                                              orderBy='startTime').execute()
        
        if not events_result:
            print('No upcoming events found.')
            return
        else:
            return events_result

    except HttpError as error:
        print('An error occurred: %s' % error)


def main(current_loop):
    source: Observable[int] = reactivex.interval(10)  # type: ignore

    def flatten(i):
        print(i)
        return reactivex.from_future(asyncio.run_coroutine_threadsafe(send_request(), loop=current_loop))

    source.pipe(
        ops.flat_map(flatten), # do async API request
        ops.map(lambda x: x['items']),  # from API response get list with events  # do async API request
        ops.flat_map(lambda x: x),  # flatten events list
        ops.map(lambda i: (i['id'], i['summary'], i['location'], i["start"]["dateTime"])),  # get only needful attributes from event
        lambda x: disaster_saver.save(x, 'calendar')
    ).subscribe(on_next=print)


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    main(loop)
    loop.run_forever()