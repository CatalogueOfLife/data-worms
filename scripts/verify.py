from config import API_URL
from config import API_USER
from config import API_PASS
from config import DATASETS
from datetime import datetime, timedelta, timezone
import pytest
import requests


SYNC_WINDOW_HOURS = 10 * 24


def get_dataset_sectors(dataset_id):
    resp = requests.get(f'{API_URL}/dataset/3/sector?datasetKey=3&limit=1000&offset=0&subjectDatasetKey={dataset_id}', auth=(API_USER, API_PASS))
    if resp.status_code != 200:
        print(f'GET /dataset/3/sector?subjectDatasetKey={dataset_id} {resp.status_code} {resp.text}')
        return []
    return [s['id'] for s in resp.json().get('result', [])]


def get_sector_sync_jobs(sector_id):
    params = {'sectorKey': sector_id, 'lane': 'SYNC'}
    resp = requests.get(f'{API_URL}/job/search', params=params)
    if resp.status_code != 200:
        print(f'GET /job/search sectorKey={sector_id} lane=SYNC {resp.status_code} {resp.text}')
    return resp


def _parse_dt(s):
    # LocalDateTime is serialized without a timezone; treat it as UTC.
    # Fractional seconds may be absent, or have up to nanosecond precision.
    if '.' in s:
        head, frac = s.split('.', 1)
        frac = frac[:6]
        s = f'{head}.{frac}' if frac else head
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


# the backend bug in which canceled sync jobs are always at the top breaks sync verification, so we need to skip them
#  https://github.com/CatalogueOfLife/backend/issues/1108
# if the job actually was canceled then it should still fail because the last successful sync will likely be too old
def skip_canceled(results):
    index = 0
    for j in results:
        if j['status'] == 'CANCELED':
            index += 1
            continue
        break
    return index


def _err(dataset, sector_id, code, msg):
    return {"id": dataset['id'], "alias": dataset['alias'],
            "sector_id": sector_id, "code": code, "msg": msg}


def test_sector_syncs_completed():
    errors = []
    now = datetime.now(timezone.utc)
    window_start = now - timedelta(hours=SYNC_WINDOW_HOURS)

    for dataset in DATASETS:
        sector_ids = get_dataset_sectors(dataset['id'])

        for sector_id in sector_ids:
            resp = get_sector_sync_jobs(sector_id)

            if resp.status_code != 200:
                errors.append(_err(dataset, sector_id, resp.status_code,
                                   "Failed to search sector sync jobs"))
                continue

            results = resp.json().get('result', [])
            index = skip_canceled(results)

            if index >= len(results):
                errors.append(_err(dataset, sector_id, resp.status_code,
                                   "No non-canceled sector sync found"))
                continue

            latest = results[index]

            if latest.get('status') != 'FINISHED':
                errors.append(_err(dataset, sector_id, resp.status_code,
                                   f"Sector sync status was {latest.get('status')!r}, not 'FINISHED'"))
                continue

            try:
                finished = _parse_dt(latest['finished'])
                if not (window_start <= finished <= now):
                    errors.append(_err(dataset, sector_id, resp.status_code,
                                       f"Sync finished {finished.isoformat()} outside {SYNC_WINDOW_HOURS}h window"))
            except (KeyError, ValueError) as e:
                errors.append(_err(dataset, sector_id, resp.status_code,
                                   f"Could not parse sync finished time: {e}"))

    if errors:
        lines = [f"  [{e['id']}] {e['alias']} sector {e['sector_id']}: {e['msg']} (HTTP {e['code']})" for e in errors]
        pytest.fail(f"{len(errors)} error(s):\n" + "\n".join(lines), pytrace=False)
