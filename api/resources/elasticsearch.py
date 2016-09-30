import rapidjson as json
import time
import logging

from api.resources.base import BaseCollectionResource, BaseSingleResource
from elasticsearch import NotFoundError
from elasticsearch_dsl import Mapping, Field
from elasticsearch_dsl import Search, Nested
from falcon import HTTPBadRequest, HTTPNotFound


class ElasticSearchMixin(object):
    _underscore_operators = {
        'exact':        'term',
        'notexact':     'term',
        'gt':           'range',
        'lt':           'range',
        'gte':          'range',
        'lte':          'range',
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
        'hasall':       'term',
        'hasany':       'terms',
        'haskey':       'term',
        'overlap':      'terms',
        'isnull': 'missing',
        'isnotnull': 'exists',
    }
    _logical_operators = {
        'or': 'should',
        'and': 'must',
        'not': 'must_not',
    }

    def serialize(self, obj):
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
                expression = self._parse_logical_op(arg, value, self._logical_operators[arg])
            else:
                expression = self._parse_tokens(self.objects_class, arg.split('__'), value,
                                                lambda n, v: {'term': {n: v, '_expand__to_dot': False}})
            if expression is not None:
                expressions.append(expression)
        result = None
        if len(expressions) > 1:
            expressions = self._group_nested(expressions, default_op)
        if len(expressions) > 1:
            result = {'bool': {default_op: expressions}}
        elif len(expressions) == 1:
            result = expressions[0] if default_op != 'must_not' else {'bool': {'must_not': expressions[0]}}
        return result

    def _group_nested(self, expressions, op):
        """
        Group all nested queries with common path, so {a__b__c, a__b__d, a__e, f} becomes {a: {b: {c, d}, e}, f}
        :param expressions: expressions returned by _parse_tokens()
        :type expressions: list[dict]

        :param op: an operator that would be used to join expressions
        :type op: str

        :return: modified expressions
        :rtype: list[dict]
        """
        expressions = list(expressions)
        while True:
            longest_path = None
            for part in expressions:
                if part.keys() == ['nested']:
                    if longest_path is None or part['nested']['path'].count('.') > longest_path.count('.'):
                        longest_path = part['nested']['path']
            if longest_path is None:
                break
            new_parts = []
            group = []
            for part in expressions:
                if part.keys() == ['nested'] and part['nested']['path'] == longest_path:
                    group.append(part['nested']['query'])
                else:
                    new_parts.append(part)
            if len(group) <= 1:
                break
            expressions = new_parts
            expressions.append({'nested': {'path': longest_path, 'query': {'bool': {op: expressions}}}})
        return expressions

    def _parse_logical_op(self, arg, value, op):
        if isinstance(value, dict):
            return self._build_filter_expressions(value, op)
        if not isinstance(value, list):
            raise HTTPBadRequest('Invalid attribute', 'Filter attribute {} is invalid'.format(arg))
        parts = []
        for subconditions in value:
            if not isinstance(subconditions, dict):
                raise HTTPBadRequest('Invalid attribute', 'Filter attribute {} is invalid'.format(arg))
            subexpressions = self._build_filter_expressions(subconditions, 'must')
            if subexpressions is not None:
                parts.append(subexpressions)
        result = None
        if len(parts) > 1:
            parts = self._group_nested(parts, op)
        if len(parts) > 1:
            result = {'bool': {op: parts}}
        elif len(parts) == 1:
            result = parts[0] if op != 'must_not' else {'bool': {'must_not': parts[0]}}
        return result

    def _parse_tokens(self, obj_class, tokens, value, default_expression=None, wrap_nested=True):
        column_name = None
        field = None
        nested_name = None
        accumulated = ''
        mapping = obj_class._doc_type.mapping
        for index, token in enumerate(tokens):
            if token == CollectionResource.PARAM_TEXT_QUERY:
                query_method = getattr(obj_class, 'get_term_query', None)
                if not callable(query_method):
                    raise HTTPBadRequest('Invalid attribute', 'Param {} is invalid, specific object '
                                                              'can\'t provide a query'.format('__'.join(tokens)))
                return query_method(self=obj_class, column_name=column_name, value=value,
                                    default_op='should' if tokens[-1] == 'or' else 'must')
            if column_name is not None:
                if token not in self._underscore_operators:
                    raise HTTPBadRequest('Invalid attribute', 'Param {} is invalid, part {} is expected to be a known '
                                                              'operator'.format('__'.join(tokens), token))
                op = self._underscore_operators[token]
                if token in ['range', 'notrange', 'in', 'notin', 'hasany', 'overlap']\
                        or (token in ['contains', 'notcontains'] and field._multi):
                    if not isinstance(value, list):
                        value = [value]
                if op in ['missing', 'exists']:
                    expression = {op: {'field': column_name, '_expand__to_dot': False}}
                elif op == 'range':
                    if token != 'range':
                        expression = {op: {column_name: {token: value}, '_expand__to_dot': False}}
                    else:
                        expression = {op: {column_name: {'gte': value[0], 'lte': value[1]}, '_expand__to_dot': False}}
                elif op == 'wildcard':
                    if token == 'contains' or token == 'notcontains':
                        if field._multi:
                            expression = {'terms': {column_name: value, '_expand__to_dot': False}}
                        else:
                            expression = {op: {column_name: '*' + value + '*', '_expand__to_dot': False}}
                    else:
                        expression = {op: {column_name: '*' + value, '_expand__to_dot': False}}
                else:
                    expression = {op: {column_name: value, '_expand__to_dot': False}}
                if op.startswith('not'):
                    expression = {'bool': {'must_not': expression}}
                if nested_name is not None and wrap_nested:
                    return {'nested': {'path': nested_name, 'query': expression}}
                return expression
            if accumulated and accumulated in mapping\
                    and isinstance(mapping[accumulated], Nested):
                nested_name = accumulated
                obj_class = mapping[accumulated]._doc_class
                mapping = Mapping(obj_class.__class__.__name__)
                for name, attr in obj_class.__dict__.items():
                    if isinstance(attr, Field):
                        mapping.field(name, attr)
                accumulated = ''
            accumulated += ('__' if accumulated else '') + token
            if accumulated in mapping\
                    and not isinstance(mapping[accumulated], Nested):
                column_name = ((nested_name + '.') if nested_name else '') + accumulated
                field = mapping[accumulated]
        if column_name is None:
            raise HTTPBadRequest('Invalid attribute', 'Param {} is invalid, it is expected to be a known '
                                                      'column name'.format('__'.join(tokens)))
        if column_name is not None and default_expression is not None:
            # if last token was a relation it's just going to be ignored
            expression = default_expression(column_name, value)
            if nested_name is not None and wrap_nested:
                return {'nested': {'path': nested_name, 'query': expression}}
            return expression
        return None

    @classmethod
    def get_match_query(cls, value, default_op):
        if isinstance(value, list):
            tq = {'query': ' '.join(['"' + v.replace('\\', '\\\\').replace('"', '\\"') + '"' for v in value]),
                  'operator': 'or' if default_op == 'should' else 'and'}
        else:
            tq = value
        return tq

    def _build_order_expressions(self, criteria):
        """
        :param criteria: criteria dictionary
        :type criteria: dict

        :return: expressions list
        :rtype: list
        """
        return criteria


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

    def get_base_query(self):
        return Search(using=self.connection,
                      index=self.objects_class._doc_type.index,
                      doc_type=self.objects_class)

    def get_queryset(self, req, resp):
        query = self.get_base_query()

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
            order_expressions = self._build_order_expressions(order)
            if order_expressions:
                query = query.sort(*order_expressions)
        return self.filter_by(query, req.params)

    def get_total_objects(self, queryset, totals):
        if not totals:
            return {}
        queryset = self._build_total_expressions(queryset, totals)
        aggs = queryset.execute()._d_.get('aggregations', {})
        aggs['count'] = {'value': queryset.execute()._d_['hits']['total']}
        result = {}
        for key, value in aggs.items():
            if 'buckets' not in value:
                result['total_' + key] = value['value']
                continue
            values = {}
            values_key = None
            for bucket in value['buckets']:
                if values_key is None:
                    for bucket_key in bucket.keys():
                        if bucket_key == 'key' or bucket_key == 'doc_count':
                            continue
                        values_key = bucket_key
                        break
                values[bucket['key']] = bucket['doc_count'] if values_key is None else bucket[values_key]['value']
            result['total_' + (values_key if values_key else 'count')] = values
        if 'total_count' not in result:
            result['total_count'] = queryset.execute()._d_['hits']['total']
        return result

    def _nest_aggregates(self, aggregates, group_by, group_limit):
        for name, expression in group_by.items():
            op = 'terms'
            options = {'field': expression, 'size': group_limit}
            if aggregates:
                aggregates = {name: {op: options, 'aggs': aggregates}}
            else:
                aggregates = {name: {op: options}}
        return aggregates

    def _build_total_expressions(self, queryset, totals):
        aggregates = {}
        group_by = {}
        group_limit = 0
        for total in totals:
            for aggregate, columns in total.items():
                if aggregate == 'count':
                    continue
                if aggregate == self.AGGR_GROUPLIMIT:
                    if not isinstance(columns, int):
                        raise HTTPBadRequest('Invalid attribute', 'Group limit option requires an integer value')
                    group_limit = columns
                    continue
                if not columns:
                    if aggregate == self.AGGR_GROUPBY:
                        raise HTTPBadRequest('Invalid attribute', 'Group by option requires at least one column name')
                    aggregates[aggregate] = {aggregate: {'field': 'id'}}
                    continue
                if not isinstance(columns, list):
                    columns = [columns]
                for column in columns:
                    expression = self._parse_tokens(self.objects_class, column.split('__'), None, lambda n, v: n,
                                                    wrap_nested=False)
                    if expression is not None:
                        if aggregate == self.AGGR_GROUPBY:
                            group_by[column] = expression
                        else:
                            aggregates[aggregate] = {aggregate: {'field': expression}}
        aggregates = self._nest_aggregates(aggregates, group_by, group_limit)
        if aggregates:
            return queryset.update_from_dict({'aggs': aggregates})
        return queryset

    def on_get(self, req, resp):
        start_seconds = time.perf_counter()
        limit = self.get_param_or_post(req, self.PARAM_LIMIT, self.max_limit)
        offset = self.get_param_or_post(req, self.PARAM_OFFSET, 0)
        totals = self.get_param_totals(req)

        queryset = self.get_queryset(req, resp)
        qs_s = time.perf_counter() - start_seconds

        object_list = self.get_object_list(queryset, int(limit) if limit is not None else None, int(offset))
        ol_s = time.perf_counter() - start_seconds
        # get totals after objects to reuse main query
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
