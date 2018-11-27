from stravalib import Client
import requests
import os
from datetime import timedelta
from celery import Celery
from celery.task import periodic_task


BACKEND = BROKER = 'redis://localhost:6379'
celery = Celery(__name__, backend=BACKEND, broker=BROKER)

DATASERVICE = "http://"+os.environ['DATASERVICE']+':5002' if 'DATASERVICE' in os.environ else "http://127.0.0.1:5002"


def fetch_all_runs():
    users = requests.get(DATASERVICE + '/users').json()['users']
    runs_fetched = {}

    for user in users:
        print(user)
        strava_token = user.get('strava_token')
        email = user['email']

        if strava_token is None:
            continue

        print('Fetching Strava for %s' % email)
        runs_fetched[user['id']] = fetch_runs(user)

    return runs_fetched


def push_to_dataservice(runs):
    print(runs)
    requests.post(DATASERVICE + '/add_runs', json=runs)


def activity2run(activity):
    """Used by fetch_runs to convert a strava entry.
    """
    run = {'strava_id': activity.id, 'name': activity.name, 'distance': activity.distance.num,
           'elapsed_time': activity.elapsed_time.total_seconds(), 'average_speed': activity.average_speed.num,
           'average_heartrate': activity.average_heartrate, 'total_elevation_gain': activity.total_elevation_gain.num,
           'start_date': activity.start_date.timestamp(), 'title': activity.name}
    if activity.description is not None:
        run['description'] = activity.description
    return run


def fetch_runs(user):
    client = Client(access_token=user['strava_token'])
    runs = []
    for activity in client.get_activities(limit=10):
        if activity.type != 'Run':
            continue
        runs.append(activity2run(activity))
    return runs


@periodic_task(run_every=timedelta(seconds=300))
def periodic_fetch():
    push_to_dataservice(fetch_all_runs())


if __name__ == '__main__':
    periodic_fetch()
