from es_celery.celery import app
import requests
import time
import os
import elasticsearch


@app.task()
def post_data(hostname, port, index, type, filename):
    with open(filename,'rb') as payload:
        # headers = {'content-type': 'application/x-www-form-urlencoded'}
        r = requests.post(f'http://{hostname}:{port}/{index}/{type}/_bulk?refresh=false&pretty',
                          data=payload, verify=False)

    output_dict = {}
    json_data = r.json()
    failed_item_ids = []
    took = json_data['took']
    if json_data['errors'] == False:
        pass
            os.remove(filename)
    else:
        for item in json_data.get('items'):
            if item.get('index'):
                index_data = item.get('index')
                if index_data.get('status') not in [200, 201]:
                    failed_item_ids.append(index_data.get("_id"))
            elif item.get('update'):
                update_data = item.get('update')
                if update_data.get('status') not in [200, 201]:
                    failed_item_ids.append(update_data.get("_id"))


    if failed_item_ids:
        output_dict['failed_item_ids'] = failed_item_ids

    output_dict['took'] = took

    return output_dict
    # print(took, errors)

@app.task()
def update_refresh_interval(hostname, port, index_name, refresh_interval):
    es = elasticsearch.Elasticsearch(host=hostname, port=port)
    es.indices.put_settings(index=index_name, body={"refresh_interval": refresh_interval})

