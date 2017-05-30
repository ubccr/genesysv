curl -XGET 'http://199.109.195.45:9200/demo_mon/demo_mon/_search?pretty=true' -d '
{
    "query": {
        "bool": {
            "filter": [{"term": {"isActive": "false"}}]}},
    "size": 1000
}
'
