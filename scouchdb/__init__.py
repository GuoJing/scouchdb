# -*- coding: utf-8 -*-

'''
A simple client for couchdb

db       -> CouchDB databsae
document -> CouchDB document

'''

import json
import pycouchdb
from pycouchdb.exceptions import NotFound

from requests.exceptions import ConnectionError

from .utils import encode

_MC_KEY_DOC = 'couch:%s:%s'


class CouchDB(object):

    def __init__(self, server, auto_create=True, mc=None):
        self.server = pycouchdb.Server(server)
        self._cache = dict()
        self.mc = mc

    def __repr__(self):
        return 'CouchDB(server=%s)' % self.server.base_url

    def _set_cache(self, name, value):
        '''
        A simple db mem cache
        '''
        if not self._cache.get(name):
            self._cache[name] = value

    def _get_cache(self, name):
        '''
        A simple db mem cache
        '''
        return self._cache.get(name, None)

    def _get_db(self, name):
        '''
        A method get database connection
        '''
        try:
            db = self._get_cache(name)
            if not db:
                db = self.server.database(name)
                if db:
                    self._set_cache(name, db)
            return db
        except NotFound:
            if self.auto_create:
                self.server.create(name)
                return self.server.database(name)
            raise Exception('couchdb database %s not found' % name)
        except ConnectionError:
            # default exception will raise exception
            # with couchdb host and port
            # we need to hide it
            raise Exception('couchdb offline')

    def get(self, db, name):
        '''
        Get value from couch db
        db    -> database name
        name  -> document name
        will be cached in redis
        '''
        _db = self._get_db(db)
        if _db:
            try:
                _doc = None
                if self.mc:
                    _doc = self.mc.get(_MC_KEY_DOC % (db, name))
                if not _doc:
                    _doc = _db.get(name)
                    _doc = json.dumps(_doc, encoding='utf-8')
                    if _doc and self.mc:
                        # if _doc is empty
                        # do not save to cache
                        self.mc.set(_MC_KEY_DOC % (db, name), _doc)
                return encode(json.loads(_doc, encoding='utf-8'))
            except NotFound:
                return None

    def delete(self, db, name):
        '''
        Delete a document from coudb db
        '''
        _db = self._get_db(db)
        _db.delete(name)
        if self.mc:
            self.mc.delete(_MC_KEY_DOC % (db, name))

    def _get_version(self, db, name):
        '''
        Need to get version first
        db    -> database name
        name  -> document name
        '''
        _v = self.get(db, name)
        if _v:
            return _v.get('_rev', None)

    def set(self, db, name, value):
        '''
        Set value to couch db
        db    -> database name
        name  -> document name
        value -> document value
        '''
        if not value:
            return
        if not isinstance(value, dict):
            raise Exception('value should be dict')
        if not name:
            raise Exception('name should not be empty')
        if not isinstance(name, str):
            raise Exception('name should be str')
        if value.get('_id'):
            if value.get('_id') != name:
                raise Exception('_id conflict')
        value['_id'] = name
        name = encode(name)
        value = encode(value)
        try:
            json.dumps(value)
        except:
            raise Exception('value should be jsonize')
        _db = self._get_db(db)
        _v = self._get_version(db, name)
        r = _v if _v else None
        if r:
            value['_rev'] = r
        r = _db.save(value)
        if self.mc:
            self.mc.delete(_MC_KEY_DOC % (db, name))
        return r.get('_rev')
