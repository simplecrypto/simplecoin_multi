import calendar
import msgpack
import redis
from elasticsearch import Elasticsearch


es = Elasticsearch()
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# add shares to elasticsearch
def add_shares(shares):
    es.index(index='minute_shares', doc_type='shares', body=shares)

i = 0
while i < r.llen('minutes'):
    packed_minute = r.lpop('minutes')
    minute = msgpack.unpackb(packed_minute)
    for user in minute:
        add_shares({'shares':user[2], 'username':(user[1]).decode(encoding='ascii'),
                             'time': calendar.timegm(user[0])})