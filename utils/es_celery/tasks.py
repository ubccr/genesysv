from import_celery.celery import app
import requests
import time
import os


@app.task
def post_data(hostname, port, index, type, filename):
    with open(filename,'rb') as payload:
        headers = {'content-type': 'application/x-www-form-urlencoded'}
        r = requests.post(f'http://{hostname}:{port}/{index}/{type}/_bulk?refresh=false&pretty',
                          data=payload, verify=False, headers=headers)

    os.remove(filename)
    json_data = r.json()
    took = json_data['took']
    errors = json_data['errors']
    return took
    # print(took, errors)
