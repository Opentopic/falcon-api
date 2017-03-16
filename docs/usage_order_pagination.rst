=======================
Ordering and pagination
=======================

This page describes how to use ordering and pagination. This only affects fetching data from collection resources.

Pagination
**********

Pagination uses `limit` and `offset` params. Resources can define a default value for limit.

Since total count is _not_ returned by default, see :doc:`/usage_aggregates` on how to request it.
This can be expensive in relational databases, so  it should be fetched only when requesting the first page of results.

Ordering
********

The `order` param allows to change order of results.

