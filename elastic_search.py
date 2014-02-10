from elasticsearch import Elasticsearch


es = Elasticsearch()


# res = es.search(index="test-index", body={"query": {"term": {'author':'kimchy'}}})
# print("Got %d Hits:" % res['hits']['total'])
# for hit in res['hits']['hits']:
#     print("%(author)s" % hit["_source"])

es.indices.refresh(index="minute_shares")

res = es.search(index="minute_shares", size="1440", fields="time,shares", body={
    "query": {
        "term": {
            'username':"DLmW4utjzP7ML8iVyoQQHB1vVsCCPPnezi"
        }
    },
    "sort": {
        "time": "desc"
    }
})
print("Got %d Hits:" % res['hits']['total'])
# print(res)
for hit in res['hits']['hits']:
    print(hit['fields'])