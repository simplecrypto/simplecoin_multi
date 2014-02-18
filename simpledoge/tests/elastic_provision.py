from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from random import randint
import calendar


es = Elasticsearch()
ic = IndicesClient(es)

addresses = ['DLmW4utjzP7ML8iVyoQQHB1vVsCCPPnezi', 'DJaFeBfSYeLBm1MAR1BxJHwS4ornrzALoy', 'DRhevw3qkAjmAjyNkSyXh2kxvrXuxktmyD']

if (ic.exists(index='minute_shares')):
    ic.delete(index='minute_shares')
if (ic.exists(index='p_hashrate')):
    ic.delete(index='p_hashrate')
if (ic.exists(index='user')):
    ic.delete(index='user')

MAPPINGS_1 = {
  'shares': {
    'properties': {
      'username': { 'type': "string", 'analyzer': 'case_sensitive', "store": "no"},
      'shares': {'type': "integer", 'store': "no"}
    }
  }
}

MAPPINGS_2 = {
  'payout_data': {
    'properties': {
      'username': { 'type': "string", 'analyzer': 'case_sensitive', "store": "no"},
      'confirmed_earnings': {'type': "integer", 'store': "no"}
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
    'mappings': MAPPINGS_1
  }
)

ic.create(
  index='user',
  body={
    'settings': SETTINGS,
    'mappings': MAPPINGS_2
  }
)

# Throw in a couple confirmed earnings to test payout
es.index(index='user', doc_type='payout_data', body={
    "username": "DLmW4utjzP7ML8iVyoQQHB1vVsCCPPnezi",
    "confirmed_earnings": 2700
})
es.index(index='user', doc_type='payout_data', body={
    "username": "DLmW4utjzP7ML8iVyoQQHB1vVsCCPPnezz",
    "confirmed_earnings": 2300
})

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

es.indices.refresh(index="minute_shares")