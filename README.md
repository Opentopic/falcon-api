Falcon API
==========

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

from api.error_handlers import JsonError
from api.middlewares.auth_middleware import AuthMiddleware
from api.middlewares.json_middleware import RequireJSON, JSONTranslator
from api.resources.index import IndexResource
from api.resources.sqlalchemy import CollectionResource, SingleResource

from sqlalchemy.ext.automap import automap_base
from sqlalchemy import create_engine

engine = create_engine("sqlite:///mydatabase.db")
Base = automap_base()
Base.prepare(engine, reflect=True)

routes = [
    ('/users', CollectionResource(Base.classes.user, engine)),
    ('/users/{id}', SingleResource(Base.classes.user, engine)),
]

app = application = falcon.API(
    middleware=[
        AuthMiddleware('/', {'project-id': 'token-value'}),
        RequireJSON(),
        JSONTranslator(),
    ]
)

for route, resource in routes:
    app.add_route(route, resource)

app.add_route('/', IndexResource([route for route, resource in routes]))
app.add_error_handler(Exception, JsonError.handle)
```
