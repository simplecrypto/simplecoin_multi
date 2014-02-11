from datetime import datetime, timedelta
import random
import redis
import uuid
import msgpack


r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.flushdb()

addresses = []
for i in range(0, 35):
    addresses.append(uuid.uuid4().hex)

# run for 10 min, randomly adding shares to addresses
for i in range(0, 144):
    minute = []
    time = (datetime.utcnow() + timedelta(0, (60*i))).replace(second=0)
    for address in random.sample(addresses, 33):
        minute.append([time.timetuple(), address, random.randint(1, 5)*256])
        print('Provisioning user {0}'.format(address))

    r.rpush('minutes', msgpack.packb(minute))
