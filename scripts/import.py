from config import API_URL
from config import API_USER
from config import API_PASS
from config import DATASETS
import requests


def import_checklist(id):
    json = {"datasetKey": id, "priority": False, "force": True}
    resp = requests.post(f'{API_URL}/importer', auth=(API_USER, API_PASS), json=json)
    print('POST /importer {}\n{}'.format(resp.status_code, resp.text))
    return resp


def test_imported():
    errors = []

    print(f'API: {API_URL}')
    print(f'USER: {API_USER}')

    for dataset in DATASETS:

        print(f"Importing {dataset['id']}: {dataset['alias']}")
        resp = import_checklist(dataset['id'])

        try:
            assert resp.status_code == 201
        except AssertionError:
            print("FAILED")
            errors.append({"id": dataset['id'], "alias":  dataset['alias'], "code": resp.status_code, "message": resp.text})

    assert errors == []
