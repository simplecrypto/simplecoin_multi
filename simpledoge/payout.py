from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient


es = Elasticsearch()
ic = IndicesClient(es)



pending_payments = es.search(index="user", doc_type="payout_data", body={
    "query": {
        "match_all": {}
    },
    "filter": {
        "range": {
            "confirmed_earnings": {
                "gte":"2500"
            }
        }
    }
})

for hit in pending_payments['hits']['hits']:
    print(hit['_source'])