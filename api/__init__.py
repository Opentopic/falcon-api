"""
This library allows to easily create RESTful APIs to various databases
as resources in a [Falcon](http://falconframework.org) application.

Supported backends include SQL (SQLAlchemy) and NoSQL (MongoDB, ElasticSearch) databases.

Typical usage include:
* using provided middleware for auth and content type negotiation when creating a Falcon application, see :ref:`api.middlewares`
* registering resources, see :ref:`api.resources`
* registering an error handler, see :ref:`api.resources.error_handlers.JsonError`
"""
