import math
from urllib.parse import parse_qs

def main():
    from pprint import pprint
    import re
    import elasticsearch
    from urllib.parse import parse_qs
    import datetime
    import json
    import os
    import hashlib
    import datetime
    import gzip

    es = elasticsearch.Elasticsearch(host="199.109.195.45")
    INDEX_NAME = 'demo_mon'
    #type_name = 'SIM_case_control'
    type_name = 'demo_mon'
    es.cluster.health(wait_for_status='yellow')



    json_file_name = 'new_data.json'
    with open(json_file_name) as f:
        content = f.read().splitlines()

    data = json.loads(''.join(content))

    for content in data:
        pprint(content)

        es.index(index=INDEX_NAME, doc_type=type_name, body=content)


if __name__ == '__main__':
    main()
