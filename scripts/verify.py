from config import API_URL
from config import API_USER
from config import API_PASS
from config import DATASETS
from datetime import datetime, timedelta, timezone
import requests


def get_dataset_sectors(dataset_id):
    sector_ids = []
    resp = requests.get(f'{API_URL}/dataset/3/sector?datasetKey=3&limit=1000&offset=0&subjectDatasetKey={dataset_id}', auth=(API_USER, API_PASS))
    sector_response = resp.json()

    for sector in sector_response['result']:
        sector_ids.append(sector['id'])
    return sector_ids


def get_sector_sync_status(sector_id):
    resp = requests.get(f'{API_URL}/dataset/3/sector/sync?sectorKey={sector_id}', auth=(API_USER, API_PASS))
    print('GET /dataset/3/sector/sync?sectorKey={} {}\n{}'.format(sector_id, resp.status_code, resp.text))
    return resp


def test_sector_syncs_completed():
    errors = []

    for dataset in DATASETS:
        sector_ids = get_dataset_sectors(dataset['id'])

        for sector_id in sector_ids:
            resp = get_sector_sync_status(sector_id)

            try:
                assert resp.status_code == 200
            except AssertionError:
                errors.append({"id": dataset['id'],
                               "alias": dataset['alias'],
                               "sector_id": sector_id,
                               "code": resp.status_code,
                               "body": resp.text,
                               "msg": "Failed to get importer status"})
            try:
                assert resp.json()['result'][0]['state'] == 'finished'
            except AssertionError:
                errors.append({"id": dataset['id'],
                               "alias": dataset['alias'],
                               "sector_id": sector_id,
                               "code": resp.status_code,
                               "body": resp.text,
                               "msg": "Sector sync not finished"})
            try:
                finished_sync_datetime = datetime.strptime(resp.json()['result'][0]['finished'], "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
                current_datetime = datetime.now(timezone.utc)
                assert current_datetime - timedelta(hours=48) <= finished_sync_datetime <= current_datetime
            except AssertionError:
                errors.append(
                    {"id": dataset['id'], "alias": dataset['alias'], "code": resp.status_code, "body": resp.text,
                     "msg": "Sync finished time was not within the last 48 hours"})

    assert errors == []
