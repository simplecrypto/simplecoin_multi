from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from random import randint
import calendar


es = Elasticsearch()
ic = IndicesClient(es)

addresses = ['DLmW4utjzP7ML8iVyoQQHB1vVsCCPPnezi', 'DJaFeBfSYeLBm1MAR1BxJHwS4ornrzALoy', 'DRhevw3qkAjmAjyNkSyXh2kxvrXuxktmyD']

ic.delete(index='minute_shares')
ic.delete(index='p_hashrate')

MAPPINGS = {
  'shares': {
    'properties': {
      'username': { 'type': "string", 'analyzer': 'case_sensitive', "store": "no"},
      'shares': {'type': "integer", 'store': "no"}
    }
  }
}

SETTINGS = {
  # custom analyzer for analyzing file paths
  'analysis': {
    'analyzer': {
      'case_sensitive': {
        'type': 'custom',
        'tokenizer': 'keyword'
      }
    }
  }
}

ic.create(
  index='minute_shares',
  body={
    'settings': SETTINGS,
    'mappings': MAPPINGS
  }
)

es.index(index='p_stats', doc_type='round_time', body={'seconds': 1200})
es.index(index='p_stats', doc_type='hash_rate', body={'hashrate':1800000})
es.index(index='p_stats', doc_type='shares', body={'shares':5000})

def add_shares(username, shares):
    es.index(index='minute_shares', doc_type='shares', body=shares)


origin = datetime.utcnow()
for i in range(0,288):
    d = (origin + timedelta(0, (60*i))).replace(second=0)
    timestamp = calendar.timegm(d.utctimetuple())
    for address in addresses:
        add_shares(address, {'shares':randint(10, 16)*256, 'username':address, 'time': timestamp})
    d = (origin + timedelta(0, (300*i))).replace(second=0)
    timestamp = calendar.timegm(d.utctimetuple())
    es.index(index='p_hashrate', doc_type='asdf', body={'hashrate':randint(50, 55)*256, 'time': timestamp})

ic.flush('p_hashrate,minute_shares')


# res = es.index(index="eric", doc_type='share', id=3, body=doc)
# print(res['ok'])
#
# # res = es.get(index="eric", doc_type='share', id='ez6-Q2daTiquOVOMIY_cvA')
# # print(res['_source'])
# #
es.indices.refresh(index="minute_shares")
#
# res = es.search(index="eric", body={"query": {"match_all": {}}})
# print("Got %d Hits:" % res['hits']['total'])
# for hit in res['hits']['hits']:
#     print("%(timestamp)s %(block)s" % hit["_source"])