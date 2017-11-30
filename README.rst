Falcon API
==========

|Build Status|

Falcon API resources for databases. See the `fulldocumentation <http://falcon-api.readthedocs.io>`.

Installation
------------

Run:

.. code:: bash

    pip install falcon_dbapi

Packages required for specific databases:

-  PostgreSQL or other RDBMS: ``SQLAlchemy``, ``alchemyjsonschema``
-  ElasticSearch: ``elasticsearch-dsl``
-  MongoDB: ``mongoengine``

Usage
-----

Below is an example app with:

-  an index of available resources
-  automapped tables
-  basic authentication using a token

.. code:: python

    import falcon

    from falcon_dbapi.middlewares.auth_middleware import AuthMiddleware
    from falcon_dbapi.middlewares.json_middleware import RequireJSON, JSONTranslator, JsonError
    from falcon_dbapi.resources.index import IndexResource
    from falcon_dbapi.resources.sqlalchemy import CollectionResource, SingleResource

    from sqlalchemy.ext.automap import automap_base
    from sqlalchemy import create_engine

    from wsgiref import simple_server

    engine = create_engine("sqlite:///mydatabase.db")
    Base = automap_base()
    Base.prepare(engine, reflect=True)

    app = application = falcon.API(
        middleware=[
            AuthMiddleware('/', {'project-id': 'token-value'}),
            RequireJSON(),
            JSONTranslator(),
        ]
    )

    for name, model in Base.classes.items():
        app.add_route('/' + name, CollectionResource(model, engine)),
        app.add_route('/' + name + '/{id}', SingleResource(model, engine)),

    app.add_route('/', IndexResource(['/' + name for name in Base.classes.keys()]))
    app.add_error_handler(Exception, JsonError.handle)

    simple_server.make_server('localhost', 8888, app).serve_forever()

Test it using httpie:

.. code:: bash

    http http://localhost:8888/

.. |Build Status| image:: https://travis-ci.org/Opentopic/falcon-api.svg?branch=master
   :target: https://travis-ci.org/Opentopic/falcon-api
