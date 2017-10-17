Falcon API
==========

[![Build Status](https://travis-ci.org/Opentopic/falcon-api.svg?branch=master)](https://travis-ci.org/Opentopic/falcon-api)

Falcon API resources for databases. See the [full documentation](http://falcon-api.readthedocs.io).

## Installation

Run:

```bash
pip install falcon-api
```

Packages required for specific databases:

* PostgreSQL or other RDBMS: `SQLAlchemy`, `alchemyjsonschema`
* ElasticSearch: `elasticsearch-dsl`
* MongoDB: `mongoengine`

## Usage

Below is an example app with:

* an index of available resources
* automapped tables
* basic authentication using a token

```python
import falcon

from api.middlewares.auth_middleware import AuthMiddleware
from api.middlewares.json_middleware import RequireJSON, JSONTranslator, JsonError
from api.resources.index import IndexResource
from api.resources.sqlalchemy import CollectionResource, SingleResource

from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine

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
```
