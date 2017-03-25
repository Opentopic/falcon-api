Falcon API
==========

Falcon API resources for databases.

Documentation Available Here:

http://jenkins.opentopic.com/job/Falcon-API/ws/docs/_build/html/index.html

## Usage

```python
from api.error_handlers import JsonError
from api.middlewares.auth_middleware import AuthMiddleware
from api.middlewares.json_middleware import RequireJSON, JSONTranslator

app = application = falcon.API(
    middleware=[
        AuthMiddleware('/', {'project-id': 'token-value'}),
        RequireJSON(),
        JSONTranslator(),
    ]
)

routes = [
    ('/models', CollectionResource(models.SomeModel, db_engine)),
    ('/models/{id}', SingleResource(models.SomeModel, db_engine)),
]

for route, resource in routes:
    app.add_route(route, resource)

app.add_error_handler(Exception, JsonError.handle)
```
