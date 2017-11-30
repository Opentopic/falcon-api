"""
This library allows to easily create RESTful APIs to various databases
as resources in a `Falcon <http://falconframework.org>`_ application.

Supported backends include SQL (SQLAlchemy) and NoSQL (MongoDB, ElasticSearch) databases.

Typical usage include:

* using provided middleware for auth and content type negotiation when creating a Falcon application,
  see :py:mod:`falcon_dbapi.middlewares`
* registering resources, see :py:mod:`falcon_dbapi.resources`
* registering an error handler, see :py:mod:`falcon_dbapi.middlewares.json_middleware.JsonError`
"""
__title__ = 'falcon_dbapi'
__author__ = 'Jan Waś (jan.was@opentopic.com)'
__license__ = 'MIT'
__version__ = '1.2.0'
