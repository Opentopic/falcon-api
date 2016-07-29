import rapidjson as json
import time
import logging

from api.resources.base import BaseCollectionResource, BaseSingleResource
from elasticsearch import NotFoundError
from elasticsearch_dsl import Search
from falcon import HTTPBadRequest, HTTPNotFound


class ElasticSearchMixin(object):
    _underscore_operators = {
        'exact':        'term',
        'notexact':     'term',
        'gt':           'range',
        'lte':          'range',
        'gte':          'range',
        'le':           'range',
        'range':        'range',
        'notrange':     'range',
        'in':           'terms',
        'notin':        'terms',
        'contains':     'wildcard',
        'notcontains':  'wildcard',
        'match':        'match',
        'notmatch':     'match',
        'startswith':   'prefix',
        'notstartswith': 'prefix',
        'endswith':     'wildcard',
        'notendswith':  'wildcard',
        'isnull': 'missing',
        'isnotnull': 'exists',
    }
    _logical_operators = {
        'or': 'should',
        'and': 'must',
        'not': 'must_not',
    }

    def serialize(self, obj):
        # TODO: unflatten relations etc: search for keys with __, group by prefix, turn into list of objects
        return obj['_source']

    def filter_by(self, query, conditions):
        expressions = self._build_filter_expressions(conditions, None)
        if expressions is None:
            return query
        return query.update_from_dict({'query': {'constant_score': {'filter': expressions}}})

    def _build_filter_expressions(self, conditions, default_op):
        """
        :param conditions: conditions dictionary
        :type conditions: dict

        :param default_op: a default operator to join all filter expressions
        :type default_op: function

        :return: expressions list
        :rtype: list
        """
        if default_op is None:
            default_op = 'must'

        expressions = []

        for arg, value in conditions.items():
            if arg in self._logical_operators:
                op = self._logical_operators[arg]
                if isinstance(value, list):
                    parts = []
                    for subconditions in value:
                        if not isinstance(subconditions, dict):
                            raise HTTPBadRequest('Invalid attribute', 'Filter attribute {} is invalid'.format(arg))
                        subexpressions = self._build_filter_expressions(subconditions, 'must')
                        if subexpressions is not None:
                            parts.append(subexpressions)
                    if len(parts) > 1:
                        expressions.append({'bool': {op: parts}})
                    elif len(parts) == 1:
                        expressions.append(parts[0] if op != 'must_not' else {'bool': {'must_not': parts[0]}})
                    continue
                if not isinstance(value, dict):
                    raise HTTPBadRequest('Invalid attribute', 'Filter attribute {} is invalid'.format(arg))
                subexpressions = self._build_filter_expressions(value, op)
                if subexpressions is not None:
                    expressions.append(subexpressions)
                continue
            expression = self._parse_tokens(self.objects_class, arg.split('__'), value, lambda n, v: {'term': {n: v}})
            if expression is not None:
                expressions.append(expression)
        result = None
        if len(expressions) > 1:
            result = {'bool': {default_op: expressions}}
        elif len(expressions) == 1:
            result = expressions[0] if default_op != 'must_not' else {'bool': {'must_not': expressions[0]}}
        return result

    def _parse_tokens(self, obj_class, tokens, value, default_expression=None):
        column_name = None
        accumulated = ''
        for index, token in enumerate(tokens):
            if column_name is not None:
                if token == CollectionResource.PARAM_TEXT_QUERY:
                    query_method = getattr(obj_class, 'get_term_query', None)
                    if not callable(query_method):
                        raise HTTPBadRequest('Invalid attribute', 'Param {} is invalid, specific object '
                                                                  'can\'t provide a query'.format('__'.join(tokens)))
                    return query_method(self=obj_class, column_name=column_name, value=value,
                                        default_op='should' if tokens[-1] == 'or' else 'must')
                if token not in self._underscore_operators:
                    raise HTTPBadRequest('Invalid attribute', 'Param {} is invalid, part {} is expected to be a known '
                                                              'operator'.format('__'.join(tokens), token))
                op = self._underscore_operators[token]
                if token in ['range', 'in']:
                    if not isinstance(value, list):
                        value = [value]
                if op in ['missing', 'exists']:
                    expression = {op: {'field': column_name}}
                elif op == 'range':
                    if token != 'range':
                        expression = {op: {column_name: {token: value}}}
                    else:
                        expression = {op: {column_name: {'gte': value[0], 'lte': value[1]}}}
                elif op == 'wildcard':
                    expression = {op: {column_name: '*' + value +
                                                    ('*' if token == 'contains' or token == 'notcontains' else '')}}
                else:
                    expression = {op: {column_name: value}}
                if op.startswith('not'):
                    expression = {'bool': {'must_not': expression}}
                return expression
            if token not in obj_class._doc_type.mapping:
                # if token is not an op it has to be a valid column
                raise HTTPBadRequest('Invalid attribute', 'Param {} is invalid, part {} is expected to be a known '
                                                          'column name'.format('__'.join(tokens), token))
            column_name = token
        if column_name is not None and default_expression is not None:
            # if last token was a relation it's just going to be ignored
            return default_expression(column_name, value)
        return None

    def get_match_query(self, value, default_op):
        if isinstance(value, list):
            tq = {'query': ' '.join(value), 'operator': 'or' if default_op == 'should' else 'and'}
        else:
            tq = value
        return tq


class CollectionResource(ElasticSearchMixin, BaseCollectionResource):
    """
    Allows to fetch a collection of a resource (GET) and to create new resource in that collection (POST).
    May be extended to allow batch operations (ex. PATCH).
    When fetching a collection (GET), following params are supported:
    * limit, offset - for pagination
    * total_count - to calculate total number of items matching filters, without pagination
    * all other params are treated as filters, syntax mimics Django filters,
      see `ElasticSearchMixin._underscore_operators`
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
        return {'total_count': queryset.execute()._d_['hits'].total}

    def on_get(self, req, resp):
        start_seconds = time.perf_counter()
        limit = self.get_param_or_post(req, self.PARAM_LIMIT, self.max_limit)
        offset = self.get_param_or_post(req, self.PARAM_OFFSET, 0)
        totals = self.get_param_totals(req)

        queryset = self.get_queryset(req, resp)
        qs_s = time.perf_counter() - start_seconds

        object_list = self.get_object_list(queryset, int(limit) if limit is not None else None, int(offset))
        ol_s = time.perf_counter() - start_seconds
        # get totals after objects to reuse already executed query
        totals = self.get_total_objects(queryset, totals)
        t_s = time.perf_counter() - start_seconds

        # use raw data from object_list and avoid unnecessary serialization
        data = object_list.execute()._d_['hits']['hits']
        d_s = time.perf_counter() - start_seconds
        serialized = [self.serialize(obj) for obj in data]
        result = {
            'results': serialized,
            'total': totals.pop('total_count') if 'total_count' in totals else None,
            'returned': len(serialized)
        }
        result.update(totals)
        self.render_response(result, req, resp)
        r_s = time.perf_counter() - start_seconds
        logging.getLogger().debug(
            'ES CollectionResource: qs {}s, ol {}s, t {}s, d {}s, r {}s'.format(qs_s, ol_s, t_s, d_s, r_s),
            extra={
                'queryset_seconds': qs_s,
                'object_list_seconds': ol_s,
                'totals_seconds': t_s,
                'data_seconds': d_s,
                'result_seconds': r_s,
            }
        )

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
