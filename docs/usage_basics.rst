======
Basics
======

Supported operations depend on HTTP verbs:

* GET - fetching collections and single resources
* POST - creating new resources in a collection
* PUT/PATCH - updating a resource
* DELETE - removing a resource
* HEAD - same as GET but returns only HTTP headers, no content
* OPTIONS - lists all allowed methods in the `Allow` HTTP header and returns resource's JSON Schema

Content type of requests and responses might be JSON or HTML, depending on which middleware is used.

Reading resources
*****************

A response to a GET request to a collection resource will contain three keys:

* `results` (plural) - a _list_ of items
* `total` - the total number of items in the collection, if `total_count` param was set to true
* `returned` - number of returned items

Total count and returned are also sent as HTTP headers: `x-api-total` and `x-api-returned`.

A single resource returns a single item under the `results` key.

Writing resources
*****************

To create or modify a resource, send one item using a POST or PUT method
to a collection (create) or a single resource (update) endpoint.

Use the OPTIONS method to see resource's attributes.

See also :doc:`/usage_relations`.

Data types
**********

JSON data types are used where possible. Dates use the ISO format (Y-m-dTH:M:SZ).

Some internal data types like `TSVECTOR` in PostgreSQL are always ignored.

Pagination
**********

Pagination uses `limit` and `offset` params. Resources can define a default value for limit.

Note: total count is _not_ returned by default, request it by setting `total_count` param to true.
This can be expensive in relational databases, so it should be fetched only when requesting the first page of results.

Ordering
********

The `order` param allows to change order of results. It can be a list of attributes.
Default order is ascending - prepend a minus (-) to an attributes name for descending order.

It's possible to use advanced expressions for ordering with the same syntax as described in :doc:`/usage_filtering`.
