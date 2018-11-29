import unittest
from unittest import mock
from datapump.datapump import fetch_all_runs, push_to_dataservice


def mocked_result(val):
    def mocked_result_fun(*args, **kwargs):
        value = val

        class MockResponse:
            def __init__(self, response=None):
                self.response = response

            def get(self, *args, **kwargs):
                return self.response

        return MockResponse(value)

    return mocked_result_fun


def test_fetch_all_runs():
    fun = mocked_result([{
        "resource_state": 2,
        "athlete": {
            "id": 134815,
            "resource_state": 1
        },
        "name": "Happy Friday",
        "distance": 24931.4,
        "moving_time": 4500,
        "elapsed_time": 4500,
        "total_elevation_gain": 0,
        "type": "Run",
        "workout_type": None,
        "id": 15450425037682,
        "external_id": "garmin_push_12345678987654321",
        "upload_id": 987654321234567891234,
        "start_date": "2018-05-02T12:15:09Z",
        "start_date_local": "2018-05-02T05:15:09Z",
        "timezone": "(GMT-08:00) America/Los_Angeles",
        "utc_offset": -25200,
        "start_latlng": None,
        "end_latlng": None,
        "location_city": None,
        "location_state": None,
        "location_country": "United States",
        "start_latitude": None,
        "start_longitude": None,
        "achievement_count": 0,
        "kudos_count": 3,
        "comment_count": 1,
        "athlete_count": 1,
        "photo_count": 0,
        "map": {
            "id": "a12345678987654321",
            "summary_polyline": None,
            "resource_state": 2
        },
        "trainer": True,
        "commute": False,
        "manual": False,
        "private": False,
        "flagged": False,
        "gear_id": "b12345678987654321",
        "from_accepted_tag": False,
        "average_speed": 5.54,
        "max_speed": 11,
        "average_cadence": 67.1,
        "average_watts": 175.3,
        "weighted_average_watts": 210,
        "kilojoules": 788.7,
        "device_watts": True,
        "has_heartrate": True,
        "average_heartrate": 140.3,
        "max_heartrate": 178,
        "max_watts": 406,
        "pr_count": 0,
        "total_photo_count": 1,
        "has_kudoed": False,
        "suffer_score": 82
    }, {
        "resource_state": 2,
        "athlete": {
            "id": 134815,
            "resource_state": 1
        },
        "name": "Bondcliff",
        "distance": 23676.5,
        "moving_time": 5400,
        "elapsed_time": 5400,
        "total_elevation_gain": 0,
        "type": "Run",
        "workout_type": None,
        "id": 123456780,
        "external_id": "garmin_push_12345678987654321",
        "upload_id": 1234567819,
        "start_date": "2018-04-30T12:35:51Z",
        "start_date_local": "2018-04-30T05:35:51Z",
        "timezone": "(GMT-08:00) America/Los_Angeles",
        "utc_offset": -25200,
        "start_latlng": None,
        "end_latlng": None,
        "location_city": None,
        "location_state": None,
        "location_country": "United States",
        "start_latitude": None,
        "start_longitude": None,
        "achievement_count": 0,
        "kudos_count": 4,
        "comment_count": 0,
        "athlete_count": 1,
        "photo_count": 0,
        "map": {
            "id": "a12345689",
            "summary_polyline": None,
            "resource_state": 2
        },
        "trainer": True,
        "commute": False,
        "manual": False,
        "private": False,
        "flagged": False,
        "gear_id": "b12345678912343",
        "from_accepted_tag": False,
        "average_speed": 4.385,
        "max_speed": 8.8,
        "average_cadence": 69.8,
        "average_watts": 200,
        "weighted_average_watts": 214,
        "kilojoules": 1080,
        "device_watts": True,
        "has_heartrate": True,
        "average_heartrate": 152.4,
        "max_heartrate": 183,
        "max_watts": 403,
        "pr_count": 0,
        "total_photo_count": 1,
        "has_kudoed": False,
        "suffer_score": 162
    }])
    fun1 = mocked_result([{
        "resource_state": 2,
        "athlete": {
            "id": 134814,
            "resource_state": 1
        },
        "name": "Happy Friday",
        "distance": 24931.4,
        "moving_time": 4500,
        "elapsed_time": 4500,
        "total_elevation_gain": 0,
        "type": "Run",
        "workout_type": None,
        "id": 154504250376823,
        "external_id": "garmin_push_12345678987654321",
        "upload_id": 987654321234567891234,
        "start_date": "2018-05-02T12:15:09Z",
        "start_date_local": "2018-05-02T05:15:09Z",
        "timezone": "(GMT-08:00) America/Los_Angeles",
        "utc_offset": -25200,
        "start_latlng": None,
        "end_latlng": None,
        "location_city": None,
        "location_state": None,
        "location_country": "United States",
        "start_latitude": None,
        "start_longitude": None,
        "achievement_count": 0,
        "kudos_count": 3,
        "comment_count": 1,
        "athlete_count": 1,
        "photo_count": 0,
        "map": {
            "id": "a12345678987654321",
            "summary_polyline": None,
            "resource_state": 2
        },
        "trainer": True,
        "commute": False,
        "manual": False,
        "private": False,
        "flagged": False,
        "gear_id": "b12345678987654321",
        "from_accepted_tag": False,
        "average_speed": 5.54,
        "max_speed": 11,
        "average_cadence": 67.1,
        "average_watts": 175.3,
        "weighted_average_watts": 210,
        "kilojoules": 788.7,
        "device_watts": True,
        "has_heartrate": True,
        "average_heartrate": 140.3,
        "max_heartrate": 178,
        "max_watts": 406,
        "pr_count": 0,
        "total_photo_count": 1,
        "has_kudoed": False,
        "suffer_score": 82
    }])
    with mock.patch('datapump.datapump.c.ApiV3', side_effect=fun):
        with mock.patch('datapump.datapump.requests') as mocked:
            mocked.get.return_value.json.return_value = {
                                        'users':
                                        [
                                            {
                                                'email': 'example@example.com',
                                                'id': 1,
                                                'strava_token': 'blabla'
                                            },
                                            {
                                                'email': 'example1@example.com',
                                                'id': 2,
                                                'strava_token': None
                                            },
                                            {
                                                'email': 'example2@example.com',
                                                'id': 3,
                                                'strava_token': 'bleble'
                                            }
                                        ]
            }
            res = fetch_all_runs()
            assert 1 in res
            assert 2 not in res
            assert 3 in res


def test_push_to_dataservice():
    with mock.patch('datapump.datapump.requests') as mocked:
        push_to_dataservice({1: [], 2: []})
        mocked.post.assert_called_once()
