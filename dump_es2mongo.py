from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
import pymongo

mongo_cli = pymongo.MongoClient(host='localhost', port=26171)
db = mongo_cli.ddb_falcon_backup
coll = db['resume']
es = Elasticsearch()

def reformater(item):
    result = {"_id": item["_id"]}
    result.update(item['_source'])
    return result

index_name = "resume_topic2"

body = {"query": {"match_all" : {}}}
t = es.search(index=index_name, scroll='1m', search_type='scan', body=body)

while True:
    try:
        m = es.scroll(scroll_id=t['_scroll_id'])
        for i in [reformater(i) for i in m['hits']['hits']]:
            coll.insert(i)
    except NotFoundError:
        print "seems all data has been dump to mongodb"
        break
    except Exception as e:
        print str(e)

