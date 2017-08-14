from pprint import pprint
import elasticsearch
es = elasticsearch.Elasticsearch(host="199.109.192.174")
INDEX_NAME = 'demo_mon'
#type_name = 'SIM_case_control'
type_name = 'demo_mon'
es.cluster.health(wait_for_status='yellow')


if True:
    if es.indices.exists(INDEX_NAME):
        print("deleting '%s' index..." % (INDEX_NAME))
        res = es.indices.delete(index = INDEX_NAME)
        print("response: '%s'" % (res))

    print("creating '%s' index..." % (INDEX_NAME))
    res = es.indices.create(index = INDEX_NAME)
    print("response: '%s'" % (res))


mapping = {
    type_name: {
        'properties': {
            'index':            {'type' : 'integer'},
            'isActive':         {'type' : 'keyword'},
            'balance':          {'type' : 'float'},
            'age':              {'type' : 'integer'},
            'eyeColor':         {'type' : 'keyword'},
            'first':            {'type' : 'keyword'},
            'last':             {'type' : 'keyword'},
            'tag':              {'type' : 'text'},
            'friend' : {
                'type' : 'nested',
                'properties' : {
                    'friend_id':    {'type' : 'integer'},
                    'friend_name':  {'type' : 'text'},
                }
            },
            'favoriteFruit':    {'type' : 'keyword'}
        }
    }
}

pprint(mapping)

es.indices.put_mapping(index=INDEX_NAME, doc_type=type_name, body=mapping)
