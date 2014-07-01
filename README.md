scouchdb
========

Simple couchdb client based on pycouchdb

pycouchdb is easy to use but api is not that clear, so I wrote a very simple tool that hide everything behind.

    from scouchdb import CouchDB

    # auto create database
    c = CouchDB(your_server_http, auto_create=True)

    c.set(database, document, value)

    c.get(database, document) == value

    c.delete(database, document)

