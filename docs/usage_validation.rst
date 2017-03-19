==========
Validation
==========

Data validation should be performed in a middleware, before processing a request.
Since the default content type is JSON and some resources provide a JSON Schema,
the easiest way is to use the `jsonschema <https://pypi.python.org/pypi/jsonschema>`_ library to do that.

The `falconjsonio <https://pypi.python.org/pypi/falconjsonio/1.0.1>`_ library already has a middleware that does that.
