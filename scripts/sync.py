from config import API_URL
from config import API_USER
from config import API_PASS
from config import DATASETS
from datetime import datetime, timedelta, timezone
import requests


def check_import_status(id):
    resp = requests.get(f'{API_URL}/importer/{id}', auth=(API_USER, API_PASS))
    print('GET /importer/{} {}\n{}'.format(id, resp.status_code, resp.text))
    return resp


def add_checklist_to_sync_queue(id):
    sync_json = {"datasetKey": id}
    resp = requests.post(f'{API_URL}/dataset/3/sector/sync', auth=(API_USER, API_PASS), json=sync_json)
    print('POST /importer {}\n{}'.format(resp.status_code, resp.text))
    return resp


def test_imported():
    errors = []

    for dataset in DATASETS:
        print(f"Checking if {dataset['id']}: {dataset['alias']} was imported...")
        resp = check_import_status(dataset['id'])
        import_status = resp.json()

        try:
            assert resp.status_code == 200
        except AssertionError:
            errors.append({"id": dataset['id'], "alias": dataset['alias'], "code": resp.status_code, "body": resp.text, "msg": "Failed to get importer status"})
        try:
            assert import_status['state'] == 'finished'
        except AssertionError:
            errors.append({"id": dataset['id'], "alias": dataset['alias'], "code": resp.status_code, "body": resp.text,
                           "msg": "Importer state was not 'finished'"})
        try:
            finished_importing_datetime = datetime.strptime(import_status['finished'], "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
            current_datetime = datetime.now(timezone.utc)
            assert current_datetime - timedelta(hours=72) <= finished_importing_datetime <= current_datetime
        except AssertionError:
            errors.append({"id": dataset['id'], "alias": dataset['alias'], "code": resp.status_code, "body": resp.text,
                           "msg": "Importer finished time was not within the last 72 hours"})
    print(errors)
    assert errors == []


def test_add_datasets_to_sync_queue():
    errors = []

    print(f'API: {API_URL}')
    print(f'USER: {API_USER}')

    for dataset in DATASETS:

        print(f"Syncing {dataset['id']}: {dataset['alias']} into CoL")
        resp = add_checklist_to_sync_queue(dataset['id'])

        try:
            assert resp.status_code == 201
        except AssertionError:
            errors.append({"id": dataset['id'], "alias":  dataset['alias'], "code": resp.status_code, "body": resp.text, "msg": "Failed to add dataset into sync queue"})

    assert errors == []
