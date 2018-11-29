from stravalib import client as c
import requests
import os
from celery import Celery

BACKEND = BROKER = 'redis://' + os.environ[
    'REDIS'] + ":6379" if 'REDIS' in os.environ else "redis://127.0.0.1:6379"
celery = Celery(__name__, backend=BACKEND, broker=BROKER)

DATA_SERVICE = "http://"+os.environ['DATA_SERVICE']+':5002' if 'DATA_SERVICE' in os.environ else "http://127.0.0.1:5002"


celery.conf.timezone = 'Europe/Rome'
celery.conf.beat_schedule = {
    'get-runs-every-five-minutes': {
        'task': 'datapump.datapump.periodic_fetch',
        'schedule': 300.0
    }
}

celery.conf.task_routes = {'datapump.datapump.periodic_fetch': {'queue': 'fetch'}}


def fetch_all_runs():
    users = requests.get(DATA_SERVICE + '/users').json()['users']
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
    requests.post(DATA_SERVICE + '/add_runs', json=runs)


def activity2run(activity):
    """Used by fetch_runs to convert a strava entry.
    """
    run = {'strava_id': activity.id, 'name': activity.name, 'distance': activity.distance.num,
           'elapsed_time': activity.elapsed_time.total_seconds(), 'average_speed': activity.average_speed.num,
           'total_elevation_gain': activity.total_elevation_gain.num,
           'start_date': activity.start_date.timestamp(), 'title': activity.name}
    if activity.description is not None:
        run['description'] = activity.description
    if activity.average_heartrate is not None:
        run['average_heartrate'] = activity.average_heartrate
    return run


def fetch_runs(user):
    client = c.Client(access_token=user['strava_token'])
    runs = []
    for activity in client.get_activities(limit=10):
        if activity.type != 'Run':
            continue
        runs.append(activity2run(activity))
    return runs


@celery.task()
def fetch_runs_for_user(user_id):  # pragma: no cover
    user_id = str(user_id)
    user = requests.get(DATA_SERVICE + '/users/' + user_id+"?secure=true").json()
    runs = fetch_runs(user)
    push_to_dataservice({user_id : runs})


@celery.task()
def periodic_fetch():  # pragma: no cover
    print("doing my job")
    push_to_dataservice(fetch_all_runs())


if __name__ == '__main__':  # pragma: no cover
    periodic_fetch()
