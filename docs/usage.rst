=====
Usage
=====

This page describes constructing HTTP queries to access resources.

For information how to register a resource in a Falcon app, see :doc:`api_resources`.

Supported operations depend on HTTP verbs:

* GET - fetching collections and single resources
* POST - creating new resources in a collection
* PUT/PATCH - updating a resource
* DELETE - removing a resource

Content type of requests and responses might be JSON or HTML, depending on which middleware is used.


.. toctree::
    :maxdepth: 2
    :glob:

    usage_*
