"""
This library allows to easily create RESTful APIs to various databases
as resources in a `Falcon <http://falconframework.org>`_ application.

Supported backends include SQL (SQLAlchemy) and NoSQL (MongoDB, ElasticSearch) databases.

Typical usage include:

* using provided middleware for auth and content type negotiation when creating a Falcon application,
  see :py:mod:`api.middlewares`
* registering resources, see :py:mod:`api.resources`
* registering an error handler, see :py:mod:`api.resources.error_handlers.JsonError`
"""
