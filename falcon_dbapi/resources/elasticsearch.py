import copy
try:
    import ujson as json
except ImportError:
    import json
from datetime import datetime, time
from decimal import Decimal

from elasticsearch import NotFoundError
from elasticsearch_dsl import Search, Nested
from falcon import HTTPBadRequest, HTTPNotFound, HTTPConflict, HTTP_NO_CONTENT

from falcon_dbapi.resources.base import BaseCollectionResource, BaseSingleResource


class ElasticSearchMixin(object):
    DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

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
        return {key: self.serialize_column(value) for key, value in obj.to_dict().items()}

    @classmethod
    def serialize_column(cls, value):
        if isinstance(value, datetime):
            return value.strftime(cls.DATETIME_FORMAT)
        elif isinstance(value, time):
            return value.isoformat()
        elif isinstance(value, Decimal):
            return float(value)
        return value

    def filter_by(self, query, conditions, order_criteria=None):
        """
        :param query: Search object
        :type query: elasticsearch.Search

        :param conditions: conditions dictionary
        :type conditions: dict

        :param order_criteria: optional order criteria
        :type order_criteria: list

        :return: modified query
        :rtype: elasticsearch.Search
        """
        expressions = self._build_filter_expressions(conditions, None)
        if expressions is None:
            return query
        if order_criteria and '_score' not in order_criteria and '-_score' not in order_criteria:
            return query.update_from_dict({'query': {'constant_score': {'filter': expressions}}})
        return query.update_from_dict({'query': expressions})

    def _build_filter_expressions(self, conditions, default_op, prevent_expand=True):
        """
        :param conditions: conditions dictionary
        :type conditions: dict

        :param default_op: a default operator to join all filter expressions
        :type default_op: str | None

        :param prevent_expand: if True, will add _expand__to_dot: False
        :type prevent_expand: bool

        :return: filter expressions
        :rtype: dict | None
        """
        if default_op is None:
            default_op = 'must'

        expressions = []

        for arg, value in conditions.items():
            if arg in self._logical_operators:
                expression = self._parse_logical_op(arg,
                                                    value,
                                                    self._logical_operators[arg],
                                                    prevent_expand=prevent_expand)
            else:
                expression = self._parse_tokens(self.objects_class,
                                                arg.split('__'),
                                                value,
                                                lambda n, v: {
                                                    'term': {n: v, '_expand__to_dot': False}
                                                    if prevent_expand else {n: v}
                                                },
                                                prevent_expand=prevent_expand)
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
        expressions = copy.deepcopy(expressions)
        while True:
            longest_path = None
            for part in expressions:
                if list(part.keys()) == ['nested']:
                    if longest_path is None or part['nested']['path'].count('.') > longest_path.count('.'):
                        longest_path = part['nested']['path']
            if longest_path is None:
                break
            new_parts = []
            group = []
            for part in expressions:
                if list(part.keys()) == ['nested'] and part['nested']['path'] == longest_path:
                    group.append(part['nested']['query'])
                else:
                    new_parts.append(part)
            if len(group) <= 1:
                break
            expressions = new_parts
            expressions.append({'nested': {'path': longest_path, 'query': {'bool': {op: group}}}})
        return expressions

    def _parse_logical_op(self, arg, value, op, prevent_expand=True):
        if isinstance(value, dict):
            return self._build_filter_expressions(value, op, prevent_expand=prevent_expand)
        if not isinstance(value, list):
            raise HTTPBadRequest('Invalid attribute', 'Filter attribute {} is invalid'.format(arg))
        parts = []
        for subconditions in value:
            if not isinstance(subconditions, dict):
                raise HTTPBadRequest('Invalid attribute', 'Filter attribute {} is invalid'.format(arg))
            subexpressions = self._build_filter_expressions(subconditions, 'must', prevent_expand=prevent_expand)
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

    def _parse_tokens(self, obj_class, tokens, value, default_expression=None, prevent_expand=True, prefer_raw=False):
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
                    expression = {op: {'field': column_name}}
                elif op == 'range':
                    if token != 'range':
                        expression = {op: {column_name: {token: value}}}
                    else:
                        expression = {op: {column_name: {'gte': value[0], 'lte': value[1]}}}
                elif op == 'wildcard':
                    if token == 'contains' or token == 'notcontains':
                        if field._multi:
                            op = 'terms'
                            expression = {op: {column_name: value}}
                        else:
                            expression = {op: {column_name: '*' + value + '*'}}
                    else:
                        expression = {op: {column_name: '*' + value}}
                else:
                    expression = {op: {column_name: value}}
                if prevent_expand:
                    expression[op]['_expand__to_dot'] = False
                if token.startswith('not'):
                    expression = {'bool': {'must_not': expression}}
                if nested_name is not None:
                    return {'nested': {'path': nested_name, 'query': expression}}
                return expression
            if accumulated and accumulated in mapping\
                    and isinstance(mapping[accumulated], Nested):
                nested_name = accumulated
                obj_class = mapping[accumulated]._doc_class
                mapping = mapping[accumulated]
                accumulated = ''
            accumulated += ('__' if accumulated else '') + token
            if accumulated in mapping and \
                    not isinstance(mapping[accumulated], Nested):
                column_name = ((nested_name + '.') if nested_name else '') + accumulated
                field = mapping[accumulated]
                if prefer_raw and getattr(field, 'fields', None) is not None and 'raw' in field.fields and \
                        field.fields['raw'].index == 'not_analyzed':
                    column_name += '.raw'
        if column_name is None:
            raise HTTPBadRequest('Invalid attribute', 'Param {} is invalid, it is expected to be a known '
                                                      'column name'.format('__'.join(tokens)))
        if column_name is not None and default_expression is not None:
            # if last token was a relation it's just going to be ignored
            expression = default_expression(column_name, value)
            if nested_name is not None:
                return {'nested': {'path': nested_name, 'query': expression}}
            return expression
        return None

    @classmethod
    def get_match_query(cls, value, default_op, boost=1):
        if isinstance(value, list):
            value = ' '.join(['"' + v.replace('\\', '\\\\').replace('"', '\\"') + '"' for v in value])
        return {'query': value,
                'operator': 'or' if default_op == 'should' else 'and',
                'boost': boost}

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
      see :py:const:`ElasticSearchMixin._underscore_operators`
    """

    def __init__(self, objects_class, connection, max_limit=None):
        """
        :param objects_class: class represent single element of object lists that's supposed to be returned

        :param connection: ElasticSearch connection or alias
        :type connection: elasticsearch.Elasticsearch | str
        """
        super(CollectionResource, self).__init__(objects_class, max_limit)
        self.connection = connection

    def get_base_query(self, req, resp):
        return Search(using=self.connection,
                      index=self.objects_class._doc_type.index,
                      doc_type=self.objects_class)

    def get_special_params(self):
        return [self.PARAM_LIMIT, self.PARAM_OFFSET, self.PARAM_TOTAL_COUNT, self.PARAM_TOTALS, self.PARAM_TEXT_QUERY]

    def get_queryset(self, req, resp):
        query = self.get_base_query(req, resp)
        conditions = {}
        if 'doc' in req.context:
            conditions = dict(req.context['doc'])
            # ignore any special params except SEARCH and ORDER
            for param in self.get_special_params():
                conditions.pop(param, None)
        conditions.update(req.params)
        if self.PARAM_SEARCH in conditions:
            search = conditions.pop(self.PARAM_SEARCH)
            try:
                conditions.update(json.loads(search) if isinstance(search, str) else search)
            except ValueError:
                raise HTTPBadRequest('Invalid attribute',
                                     'Value of {} filter attribute is invalid'.format(self.PARAM_SEARCH))

        order = conditions.pop(self.PARAM_ORDER, None)
        if not order:
            return self.filter_by(query, conditions)

        if isinstance(order, str):
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
        return self.filter_by(query, conditions, order_criteria=order_expressions)

    @classmethod
    def flatten_aggregate(cls, key, value):
        if 'buckets' not in value:
            if key in ('nested', 'filtered'):
                for subkey, subvalue in value.items():
                    if subkey == 'doc_count':
                        continue
                    return cls.flatten_aggregate(subkey, subvalue)
                raise Exception('Empty nested or filtered aggregate')
            return key, value['value'] if 'value' in value else value
        values = {}
        values_key = None
        result_key = None
        agg_name = None
        for bucket in value['buckets']:
            if values_key is None:
                for bucket_key in bucket.keys():
                    if bucket_key == 'key' or bucket_key == 'key_as_string' or bucket_key == 'doc_count':
                        continue
                    values_key = agg_name = bucket_key
                    break
            if result_key is None:
                result_key = 'key_as_string' if 'key_as_string' in bucket else 'key'
            if values_key in ('nested', 'filtered'):
                if list(bucket[values_key].keys()) == ['doc_count']:
                    agg_name = 'count'
                    values[str(bucket[result_key])] = bucket[values_key]['doc_count']
                else:
                    for subkey, subvalue in bucket[values_key].items():
                        if subkey == 'doc_count':
                            continue
                        agg_name, values[str(bucket[result_key])] = cls.flatten_aggregate(subkey, subvalue)
            else:
                values[str(bucket[result_key])] = bucket['doc_count'] if values_key is None else \
                    bucket[values_key]['value']
        return (agg_name or 'count'), values

    def get_total_objects(self, queryset, totals):
        if not totals:
            return {}
        queryset = self._build_total_expressions(queryset, totals)
        aggs = queryset.execute()._d_.get('aggregations', {})
        result = {}
        for key, value in aggs.items():
            result_key, result_value = self.flatten_aggregate(key, value)
            result['total_' + result_key] = result_value
        if 'total_count' not in result:
            result['total_count'] = queryset.execute()._d_['hits']['total']
        return result

    def _nest_aggregates(self, aggregates, group_by):
        for name, expression in group_by[::-1]:
            if aggregates:
                if 'terms' in expression:
                    expression['terms']['order'] = dict.fromkeys(aggregates.keys(), 'desc')
                expression['aggs'] = aggregates
            aggregates = {name: expression}
        return aggregates

    def _build_total_expressions(self, queryset, totals):
        aggregates = {}
        nested_groups = {}
        nested_aggs = {}
        group_by = []
        group_limit = 0
        # we need to search for group_limit first
        for total in totals:
            for aggregate, columns in total.items():
                if aggregate != self.AGGR_GROUPLIMIT:
                    continue
                if not isinstance(columns, int):
                    raise HTTPBadRequest('Invalid attribute', 'Group limit option requires an integer value')
                group_limit = columns
                break
        # next need to find groups for proper nesting, if any
        for total in totals:
            for aggregate, columns in total.items():
                if aggregate != self.AGGR_GROUPBY:
                    continue
                if not columns:
                    raise HTTPBadRequest('Invalid attribute', 'Group by option requires at least one column name')
                if not isinstance(columns, list):
                    columns = [columns]
                for column in columns:
                    if isinstance(column, dict):
                        expression = self._build_filter_expressions(column, 'must', prevent_expand=False)
                    else:
                        expression = self._parse_tokens(self.objects_class, column.split('__'), None, lambda n, v: n,
                                                        prefer_raw=True)
                    if expression is None:
                        continue
                    if isinstance(expression, dict) and list(expression.keys()) == ['nested']:
                        if expression['nested']['path'] not in nested_groups:
                            nested_groups[expression['nested']['path']] = \
                                ('nested', {'nested': {'path': expression['nested']['path']}})
                            group_by.append(nested_groups[expression['nested']['path']])
                        expression = expression['nested']['query']
                    if isinstance(column, dict):
                        group_by.append(('filtered', {'filter': expression}))
                    else:
                        group_by.append((column, {'terms': {'field': expression,
                                                            'size': group_limit}}))
                break
        # at last process normal aggregates
        for total in totals:
            for aggregate, columns in total.items():
                if aggregate == 'count' or aggregate == self.AGGR_GROUPLIMIT or aggregate == self.AGGR_GROUPBY:
                    continue
                if not columns:
                    aggregates[aggregate] = {aggregate: {'field': 'id'}}
                    continue
                if not isinstance(columns, list):
                    columns = [columns]
                for column in columns:
                    if isinstance(column, dict):
                        expression = self._build_filter_expressions(column, 'must', prevent_expand=False)
                    else:
                        expression = self._parse_tokens(self.objects_class, column.split('__'), None, lambda n, v: n,
                                                        prefer_raw=True)
                    if expression is None:
                        continue
                    if isinstance(expression, dict) and list(expression.keys()) == ['nested']:
                        if expression['nested']['path'] not in nested_groups:
                            nested_aggs[expression['nested']['path']] = \
                                ('nested', {'nested': {'path': expression['nested']['path']}})
                        expression = expression['nested']['query']
                    aggregates[aggregate] = {aggregate: {'field': expression}}
                    if nested_aggs:
                        aggregates = self._nest_aggregates(aggregates, list(nested_aggs.values()))
                        nested_aggs = {}
        aggregates = self._nest_aggregates(aggregates, group_by)
        if aggregates:
            return queryset.update_from_dict({'aggs': aggregates})
        return queryset

    def get_data(self, req, resp):
        limit = self.get_param_or_post(req, self.PARAM_LIMIT, self.max_limit)
        if limit is not None:
            limit = int(limit)
        offset = self.get_param_or_post(req, self.PARAM_OFFSET, 0)
        totals = self.get_param_totals(req)

        queryset = self.get_queryset(req, resp)
        data = self.get_object_list(queryset, limit, int(offset))
        object_list = None
        if limit != 0:
            object_list = [item['_source'] for item in data.execute()._d_['hits']['hits']] if data else []
        # get totals after objects to reuse main query
        totals = self.get_total_objects(queryset, totals)

        return object_list, totals

    def on_get(self, req, resp):
        object_list, totals = self.get_data(req, resp)

        # use raw data from object_list and avoid unnecessary serialization
        result = {'results': object_list or [],
                  'total': totals.pop('total_count') if 'total_count' in totals else None,
                  'returned': len(object_list or [])}
        result.update(totals)
        headers = {'x-api-total': str(result['total']) if result['total'] is not None else '',
                   'x-api-returned': str(result['returned'])}
        resp.set_headers(headers)
        self.render_response(result, req, resp)

    def on_head(self, req, resp):
        object_list, totals = self.get_data(req, resp)

        # use raw data from object_list and avoid unnecessary serialization
        headers = {'x-api-total': str(totals.pop('total_count')) if 'total_count' in totals else '',
                   'x-api-returned': str(len(object_list or []))}
        resp.set_headers(headers)
        resp.status = HTTP_NO_CONTENT

    def create(self, req, resp, data):
        resource = self.objects_class(**data)
        resource.save(using=self.connection)
        return self.serialize(resource)


class SingleResource(ElasticSearchMixin, BaseSingleResource):
    """
    Allows to fetch a single resource (GET) and to update (PATCH, PUT) or remove it (DELETE).
    When fetching a resource (GET).
    """

    def __init__(self, objects_class, connection):
        """
        :param objects_class: class represent single element of object lists that suppose to be returned
        :param connection: ElasticSearch connection or alias
        :type connection: elasticsearch.Elasticsearch | str
        """
        super(SingleResource, self).__init__(objects_class)
        self.connection = connection

    def get_object(self, req, resp, path_params, for_update=False):
        try:
            obj = self.objects_class.get(*path_params, using=self.connection)
        except NotFoundError:
            raise HTTPNotFound()
        return obj

    def update(self, req, resp, data, obj):
        obj.update(data)
        return self.serialize(obj)

    def delete(self, req, resp, obj):
        deleted = obj.delete(using=self.connection)
        if deleted == 0:
            raise HTTPConflict('Conflict', 'Resource found but conditions violated')
