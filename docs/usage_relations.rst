=========
Relations
=========

This page describes fetching relations data in GET requests to collection resources
and saving relations data in POST requests when creating new resources.

Fetching
********

To request relations data simply pass a comma separated list of relations in the `relations` param.

To fetch all relations, use the `_all` value.

Note: this only allows fetching directly related objects. For deeper serialisation,
override the :py:meth:`falcon_dbapi.resources.base.BaseResource.serialize()` method.

.. code-block:: python

    @classmethod
    def serialize(cls, obj, relations_level=1, *args, **kwargs):
        if isinstance(obj, models.Article):
            relations_level += 1
        return super(ArticleMixin, cls).serialize(obj, relations_level=relations_level, *args, **kwargs)

Creating
********

Relation data can be included in documents when sending them in POST requests. Either include primary keys of existing objects
to create an association or full objects without primary keys to create new objects before associating them.
There's no way to perform lookups using attributes other than primary keys.
