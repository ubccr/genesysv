curl -XGET 'http://199.109.192.174:9200/demo_mon/demo_mon/_search?pretty=true' -d '
{
    "query": {
        "bool": {
            "filter": [{"term": {"isActive": "true"}}]}},
    "size": 1000
}
'
