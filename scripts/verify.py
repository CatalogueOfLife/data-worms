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


# the backend bug in which canceled sync jobs are always at the top breaks sync verification, so we need to skip them
#  https://github.com/CatalogueOfLife/backend/issues/1108
# if the job actually was canceled then it should still fail because the last successful sync will likely be too old
def skip_canceled(json):
    index = 0
    for j in json['result']:
        if j['state'] == 'canceled':
            index = index + 1
            continue
        else:
            break
    return index


def test_sector_syncs_completed():
    errors = []

    for dataset in DATASETS:
        sector_ids = get_dataset_sectors(dataset['id'])

        for sector_id in sector_ids:
            resp = get_sector_sync_status(sector_id)
            index = skip_canceled(resp.json())
            print('INDEX', index)

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
                assert resp.json()['result'][index]['state'] == 'finished'
            except AssertionError:
                errors.append({"id": dataset['id'],
                               "alias": dataset['alias'],
                               "sector_id": sector_id,
                               "code": resp.status_code,
                               "body": resp.text,
                               "msg": "Sector sync not finished"})
            try:
                finished_sync_datetime = datetime.strptime(resp.json()['result'][index]['finished'], "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
                current_datetime = datetime.now(timezone.utc)
                print(current_datetime - timedelta(hours=48))
                print(finished_sync_datetime)
                print(current_datetime)
                assert current_datetime - timedelta(hours=48) <= finished_sync_datetime <= current_datetime
            except AssertionError:
                errors.append(
                    {"id": dataset['id'], "alias": dataset['alias'], "code": resp.status_code, "body": resp.text,
                     "msg": "Sync finished time was not within the last 48 hours"})

    assert errors == []
