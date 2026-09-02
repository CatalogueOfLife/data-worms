from config import API_URL
from config import API_USER
from config import API_PASS
from config import DATASETS
from datetime import datetime, timedelta, timezone
import pytest
import requests


def get_latest_import(dataset_id):
    params = {'datasetKey': dataset_id, 'lane': 'IMPORT', 'limit': 1}
    resp = requests.get(f'{API_URL}/job/search', params=params)
    if resp.status_code != 200:
        print(f'GET /job/search datasetKey={dataset_id} lane=IMPORT {resp.status_code} {resp.text}')
    return resp


def add_checklist_to_sync_queue(id):
    sync_json = {"datasetKey": id}
    resp = requests.post(f'{API_URL}/dataset/3/sector/sync', auth=(API_USER, API_PASS), json=sync_json)
    if resp.status_code != 201:
        print(f'POST /dataset/3/sector/sync {resp.status_code} {resp.text}')
    return resp


def _parse_dt(s):
    # LocalDateTime is serialized without a timezone; treat it as UTC.
    # Fractional seconds may be absent, or have up to nanosecond precision.
    if '.' in s:
        head, frac = s.split('.', 1)
        frac = frac[:6]
        s = f'{head}.{frac}' if frac else head
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def _report(errors):
    lines = [f"  [{e['id']}] {e['alias']}: {e['msg']} (HTTP {e['code']})" for e in errors]
    pytest.fail(f"{len(errors)} error(s):\n" + "\n".join(lines), pytrace=False)


def test_imported():
    errors = []

    for dataset in DATASETS:
        resp = get_latest_import(dataset['id'])

        if resp.status_code != 200:
            errors.append({"id": dataset['id'], "alias": dataset['alias'],
                           "code": resp.status_code,
                           "msg": "Failed to search import jobs"})
            continue

        results = resp.json().get('result', [])
        if not results:
            errors.append({"id": dataset['id'], "alias": dataset['alias'],
                           "code": resp.status_code,
                           "msg": "No import job found"})
            continue

        job = results[0]

        if job.get('status') != 'FINISHED':
            errors.append({"id": dataset['id'], "alias": dataset['alias'],
                           "code": resp.status_code,
                           "msg": f"Import status was {job.get('status')!r}, not 'FINISHED'"})
            continue

        try:
            finished_importing_datetime = _parse_dt(job['finished'])
            current_datetime = datetime.now(timezone.utc)
            assert current_datetime - timedelta(hours=192) <= finished_importing_datetime <= current_datetime
        except (AssertionError, KeyError, ValueError):
            errors.append({"id": dataset['id'], "alias": dataset['alias'],
                           "code": resp.status_code,
                           "msg": "Import finished time was not within the last 192 hours"})

    if errors:
        _report(errors)


def test_add_datasets_to_sync_queue():
    errors = []

    for dataset in DATASETS:
        resp = add_checklist_to_sync_queue(dataset['id'])

        if resp.status_code != 201:
            errors.append({"id": dataset['id'], "alias": dataset['alias'],
                           "code": resp.status_code,
                           "msg": "Failed to add dataset into sync queue"})

    if errors:
        _report(errors)
