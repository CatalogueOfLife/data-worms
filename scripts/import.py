from config import API_URL
from config import API_USER
from config import API_PASS
from config import DATASETS
import pytest
import requests


def import_checklist(id):
    json = {"datasetKey": id, "priority": False, "force": True}
    resp = requests.post(f'{API_URL}/importer', auth=(API_USER, API_PASS), json=json)
    if resp.status_code != 201:
        print(f'POST /importer {resp.status_code} {resp.text}')
    return resp


def _report(errors):
    lines = [f"  [{e['id']}] {e['alias']}: {e['msg']} (HTTP {e['code']})" for e in errors]
    pytest.fail(f"{len(errors)} error(s):\n" + "\n".join(lines), pytrace=False)


def test_imported():
    errors = []

    for dataset in DATASETS:
        resp = import_checklist(dataset['id'])

        if resp.status_code != 201:
            errors.append({"id": dataset['id'], "alias": dataset['alias'],
                           "code": resp.status_code,
                           "msg": "Failed to schedule import"})

    if errors:
        _report(errors)
