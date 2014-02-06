import simpledoge
import json
import yaml
import unittest
import base64

from pprint import pprint
from simpledoge import root, db


class UnitTest(unittest.TestCase):
    """ Represents a set of tests that only need the database iniailized, but
    no fixture data """

    def tearDown(self):
        db.session.rollback()
        db.drop_all()

    def setup_db(self):
        self.db.drop_all()
        self.db.create_all()



class APITest(UnitTest):
    def post(self, uri, status_code, params=None, has_data=True, headers=None,
             success=True, typ='post'):
        if typ == 'get' and params:
            for p in params:
                if isinstance(params[p], dict) or isinstance(params[p], list):
                    params[p] = json.dumps(params[p])

        if headers is None:
            headers = {}

        if typ == 'get':
            response = self.client.get(
                uri,
                query_string=params,
                headers=headers)
        else:
            response = getattr(self.client, typ)(
                uri,
                data=json.dumps(params),
                headers=headers,
                content_type='application/json')
        print(response.status_code)
        print(response.data)
        j = json.loads(response.data.decode('utf8'))
        pprint(j)
        assert response.status_code == status_code
        if has_data:
            assert response.data
        if success and status_code == 200:
            assert j['success']
        else:
            assert not j['success']
        return j

    def patch(self, uri, status_code, **kwargs):
        return self.post(uri, status_code, typ='patch', **kwargs)

    def put(self, uri, status_code, **kwargs):
        return self.post(uri, status_code, typ='put', **kwargs)

    def delete(self, uri, status_code, **kwargs):
        return self.post(uri, status_code, typ='delete', **kwargs)

    def get(self, uri, status_code, **kwargs):
        return self.post(uri, status_code, typ='get', **kwargs)
