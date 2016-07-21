import json

from api.resources.base import BaseCollectionResource, BaseSingleResource
from elasticsearch import NotFoundError
from elasticsearch_dsl import Search
from falcon import HTTPBadRequest, HTTPNotFound


class ElasticSearchMixin(object):
    def filter_by(self, query, conditions):
        return query

    def serialize(self, obj):
        return obj.to_dict()


class CollectionResource(ElasticSearchMixin, BaseCollectionResource):
    """
    Allows to fetch a collection of a resource (GET) and to create new resource in that collection (POST).
    May be extended to allow batch operations (ex. PATCH).
    When fetching a collection (GET), following params are supported:
    * limit, offset - for pagination
    * total_count - to calculate total number of items matching filters, without pagination
    * all other params are treated as filters, syntax mimics Django filters, see `ElasticSearchMixin._underscore_operators`
    User input can be validated by attaching the `falconjsonio.schema.request_schema()` decorator.
    """

    def __init__(self, objects_class, connection, max_limit=None):
        """
        :param objects_class: class represent single element of object lists that's supposed to be returned
        :param connection: ElasticSearch connection or alias
        :type connection: elasticsearch.Elasticsearch | str
        """
        super(CollectionResource, self).__init__(objects_class, max_limit)
        self.connection = connection

    def get_queryset(self, req, resp):
        query = Search(using=self.connection)
        query = query.doc_type(self.objects_class)

        if self.PARAM_SEARCH in req.params:
            try:
                req.params.update(json.loads(req.params.pop(self.PARAM_SEARCH)))
            except ValueError:
                raise HTTPBadRequest('Invalid attribute',
                                     'Value of {} filter attribute is invalid'.format(self.PARAM_SEARCH))
        order = self.get_param_or_post(req, self.PARAM_ORDER)
        if order:
            if (order[0] == '{' and order[-1] == '}') or (order[0] == '[' and order[-1] == ']'):
                try:
                    order = json.loads(order)
                except ValueError:
                    # not valid json, ignore and try to parse as an ordinary list of attributes
                    pass
            if not isinstance(order, list) and not isinstance(order, dict):
                order = [order]
            query = query.sort(*order)
        return self.filter_by(query, req.params)

    def get_total_objects(self, queryset, totals):
        if not totals:
            return {}
        for total in totals:
            if len(total) > 1 or 'count' not in total or total['count'] is not None:
                raise HTTPBadRequest('Invalid attribute', 'Only _count_ is supported in the _totals_ param')
        return {'total_count': queryset.execute().hits.total}

    def create(self, req, resp, data):
        raise NotImplementedError


class SingleResource(ElasticSearchMixin, BaseSingleResource):
    """
    Allows to fetch a single resource (GET) and to update (PATCH, PUT) or remove it (DELETE).
    When fetching a resource (GET).
    User input can be validated by attaching the `falconjsonio.schema.request_schema()` decorator.
    """

    def __init__(self, objects_class, connection):
        """
        :param objects_class: class represent single element of object lists that suppose to be returned
        :param connection: ElasticSearch connection or alias
        :type connection: elasticsearch.Elasticsearch | str
        """
        super(SingleResource, self).__init__(objects_class)
        self.connection = connection

    def get_object(self, req, resp, path_params):
        try:
            obj = self.objects_class.get(*path_params, using=self.connection)
        except NotFoundError:
            raise HTTPNotFound()
        return obj

    def update(self, req, resp, data, obj):
        raise NotImplementedError
