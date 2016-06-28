from contextlib import contextmanager
from datetime import datetime, time
from decimal import Decimal

import falcon
import json
from falcon import HTTPConflict, HTTPBadRequest, HTTPNotFound
from sqlalchemy import inspect, func
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.orm import sessionmaker, subqueryload, aliased
from sqlalchemy.orm.base import MANYTOONE
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.sql import sqltypes, operators, extract
from sqlalchemy.sql.expression import and_, or_, not_
from sqlalchemy.sql.functions import Function

from api.resources.base import BaseCollectionResource, BaseSingleResource


@contextmanager
def session_scope(db_engine):
    """
    Provide a scoped db session for a series of operarions.
    The session is created immediately before the scope begins, and is closed
    on scope exit.
    """
    db_session = sessionmaker(bind=db_engine)()
    try:
        yield db_session
        db_session.commit()
    except:
        db_session.rollback()
        raise
    finally:
        db_session.close()


class AlchemyMixin(object):
    """
    Provides serialize and deserialize methods to convert between JSON and SQLAlchemy datatypes.
    """
    MULTIVALUE_SEPARATOR = ','
    PARAM_RELATIONS = 'relations'
    PARAM_RELATIONS_ALL = '_all'

    _underscore_operators = {
        'exact':        operators.eq,
        'gt':           operators.gt,
        'lte':          operators.lt,
        'gte':          operators.ge,
        'le':           operators.le,
        'range':        operators.between_op,
        'in':           operators.in_op,
        'contains':     operators.contains_op,
        'iexact':       operators.ilike_op,
        'startswith':   operators.startswith_op,
        'endswith':     operators.endswith_op,
        'istartswith': lambda c, x: c.ilike(x.replace('%', '%%') + '%'),
        'iendswith': lambda c, x: c.ilike('%' + x.replace('%', '%%')),
        'isnull': lambda c, x: x and c is not None or c is None,
        'year': lambda c, x: extract('year', c) == x,
        'month': lambda c, x: extract('month', c) == x,
        'day': lambda c, x: extract('day', c) == x,
        'func': Function,
    }
    _logical_operators = {
        'or': or_,
        'and': and_,
        'not': not_,
    }

    def serialize(self, obj, skip_primary_key=False, skip_foreign_keys=False, relations_level=1, relations_ignore=None,
                  relations_include=None):
        """
        Converts the object to a serializable dictionary.
        :param obj: the object to serialize

        :param skip_primary_key: should primary keys be skipped
        :type skip_primary_key: bool

        :param skip_foreign_keys: should foreign keys be skipped
        :type skip_foreign_keys: bool

        :param relations_level: how many levels of relations to serialize
        :type relations_level: int

        :param relations_ignore: relationship names to ignore
        :type relations_ignore: list

        :param relations_include: relationship names to include
        :type relations_include: list

        :return: a serializable dictionary
        :rtype: dict
        """
        data = {}
        data = self.serialize_columns(obj, data, skip_primary_key, skip_foreign_keys)
        if relations_level > 0:
            if relations_ignore is None:
                relations_ignore = list(getattr(self, 'serialize_ignore', []))
            if relations_include is None and hasattr(self, 'serialize_include'):
                relations_include = list(getattr(self, 'serialize_include'))
            data = self.serialize_relations(obj, data, relations_level, relations_ignore, relations_include)
        return data

    def serialize_columns(self, obj, data, skip_primary_key=False, skip_foreign_keys=False):
        columns = inspect(obj).mapper.columns
        for key, column in columns.items():
            if skip_primary_key and column.primary_key:
                continue
            if skip_foreign_keys and len(column.foreign_keys):
                continue
            data[key] = self.serialize_column(column, getattr(obj, key))

        return data

    def serialize_column(self, column, value):
        if isinstance(value, datetime):
            return value.strftime('%Y-%m-%dT%H:%M:%SZ')
        elif isinstance(value, time):
            return value.isoformat()
        elif isinstance(value, Decimal):
            return float(value)
        return value

    def serialize_relations(self, obj, data, relations_level=1, relations_ignore=None, relations_include=None):
        mapper = inspect(obj).mapper
        for relation in mapper.relationships:
            if relation.key in relations_ignore\
                    or (relations_include is not None and relation.key not in relations_include):
                continue
            rel_obj = getattr(obj, relation.key)
            if rel_obj is None:
                continue
            relations_ignore = [] if relations_ignore is None else list(relations_ignore)
            if relation.back_populates:
                relations_ignore.append(relation.back_populates)
            if relation.direction == MANYTOONE:
                data[relation.key] = self.serialize(rel_obj, relations_level=relations_level - 1,
                                                    relations_ignore=relations_ignore,
                                                    relations_include=relations_include)
            elif not relation.uselist:
                data.update(self.serialize(rel_obj, skip_primary_key=True, relations_level=relations_level - 1,
                                           relations_ignore=relations_ignore,
                                           relations_include=relations_include))
            else:
                data[relation.key] = {
                    rel.id: self.serialize(rel, skip_primary_key=True, relations_level=relations_level - 1,
                                           relations_ignore=relations_ignore,
                                           relations_include=relations_include)
                    for rel in rel_obj
                }
        return data

    def deserialize(self, data, mapper=None):
        """
        Converts incoming data to internal types. Detects relation objects. Moves one to one relation attributes
        to a separate key. Silently skips unknown attributes.

        :param data: incoming data
        :type data: dict

        :param mapper: mapper, if None, mapper of the main object class will be used
        :type mapper: sqlalchemy.orm.mapper.Mapper

        :return: data with correct types
        :rtype: dict
        """
        attributes = {}

        if data is None:
            return attributes

        def is_int(s):
            try:
                int(s)
            except ValueError:
                return False
            return True

        if mapper is None:
            mapper = inspect(self.objects_class)
        for key, value in data.items():
            if key in mapper.relationships:
                rel_mapper = mapper.relationships[key].mapper
                # handle a special case, when value is a dict with only all integer keys, then convert it to a list
                if isinstance(value, dict) and all(is_int(pk) for pk in value.keys()):
                    replacement = []
                    for pk, attrs in value.items():
                        attrs[rel_mapper.primary_key[0].name] = pk
                        replacement.append(attrs)
                    value = replacement
                if isinstance(value, dict):
                    attributes[key] = self.deserialize(value, rel_mapper)
                elif isinstance(value, list):
                    attributes[key] = []
                    for item in value:
                        if isinstance(item, dict):
                            attributes[key].append(self.deserialize(item, rel_mapper))
                        else:
                            attributes[key].append(item)
                else:
                    attributes[key] = value
            elif key in mapper.columns:
                attributes[key] = self.deserialize_column(mapper.columns[key], value)
            else:
                for relation in mapper.relationships:
                    if relation.direction == MANYTOONE or relation.uselist or key not in relation.mapper.columns:
                        continue
                    if relation.key not in attributes:
                        attributes[relation.key] = []
                    attributes[relation.key].append(self.deserialize_column(relation.mapper.columns[key], value))

        return attributes

    def deserialize_column(self, column, value):
        if value is None:
            return None
        if isinstance(column.type, sqltypes.DateTime):
            return datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ')
        if isinstance(column.type, sqltypes.Time):
            hour, minute, second = value.split(':')
            return time(int(hour), int(minute), int(second))
        if isinstance(column.type, sqltypes.Integer):
            return int(value)
        if isinstance(column.type, sqltypes.Float):
            return float(value)
        return value

    def filter_by(self, query, conditions):
        """
        :param query: SQLAlchemy Query object
        :type query: sqlalchemy.orm.query.Query

        :param conditions: conditions dictionary
        :type conditions: dict

        :return: modified query
        :rtype: sqlalchemy.orm.query.Query
        """
        return self._filter_or_exclude(query, conditions)

    def exclude_by(self, query, conditions):
        """
        :param query: SQLAlchemy Query object
        :type query: sqlalchemy.orm.query.Query

        :param conditions: conditions dictionary
        :type conditions: dict

        :return: modified query
        :rtype: sqlalchemy.orm.query.Query
        """
        return self._filter_or_exclude(query, {'not': {'and': conditions}})

    def _filter_or_exclude(self, query, conditions, default_op=None):
        """
        :param query: SQLAlchemy Query object
        :type query: sqlalchemy.orm.query.Query

        :param conditions: conditions dictionary
        :type conditions: dict

        :param default_op: a default operator to join all filter expressions
        :type default_op: function

        :return: modified query
        :rtype: sqlalchemy.orm.query.Query
        """
        relationships = {
            'aliases': {},
            'join_chains': [],
        }
        expressions = self._build_filter_expressions(conditions, default_op, relationships)
        longest_chains = []
        for chain_a, chain_a_ext in relationships['join_chains']:
            is_longest = True
            for chain_b, chain_b_ext in relationships['join_chains']:
                if chain_a == chain_b:
                    continue
                if set(chain_a).issubset(chain_b):
                    is_longest = False
                    continue
            if is_longest:
                longest_chains.append(chain_a_ext)
        if longest_chains:
            query = query.distinct()
            for chain in longest_chains:
                for alias, relation in chain:
                    query = query.join((alias, relation), from_joinpoint=True)
                query = query.reset_joinpoint()
        if expressions is not None:
            query = query.filter(expressions)
        return query

    def _build_filter_expressions(self, conditions, default_op, relationships):
        column = None
        column_name = None
        obj_class = self.objects_class
        mapper = inspect(obj_class)
        column_alias = None
        join_chain = []
        join_chain_ext = []

        if default_op is None:
            default_op = and_

        expressions = []

        for arg, value in conditions.items():
            if arg in self._logical_operators:
                op = self._logical_operators[arg]
                if isinstance(value, list):
                    parts = []
                    for subconditions in value:
                        if not isinstance(subconditions, dict):
                            raise HTTPBadRequest('Invalid attribute', 'Filter attribute {} is invalid'.format(arg))
                        subexpressions = self._build_filter_expressions(subconditions, and_, relationships)
                        parts.append(subexpressions)
                    if len(parts) > 1:
                        expressions.append(op(*parts) if op != not_ else not_(and_(*parts)))
                    elif len(parts) == 1:
                        expressions.append(parts[0] if op != not_ else not_(parts[0]))
                    continue
                if not isinstance(value, dict):
                    raise HTTPBadRequest('Invalid attribute', 'Filter attribute {} is invalid'.format(arg))
                subexpressions = self._build_filter_expressions(value, self._logical_operators[arg], relationships)
                expressions.append(subexpressions)
                continue
            tokens = arg.split('__')
            for token in tokens:
                if column_name is not None and token in self._underscore_operators:
                    op = self._underscore_operators[token]
                    if op in [operators.between_op, operators.in_op]:
                        value = value.split(self.MULTIVALUE_SEPARATOR)
                    if isinstance(value, list):
                        value = list(map(lambda x: self.deserialize_column(column, x), value))
                    else:
                        value = self.deserialize_column(column, value)
                    if op == Function:
                        expressions.append(Function(tokens[-1], column_name, value))
                    else:
                        expressions.append(op(column_name, value))
                    # reset column_name, otherwise a default eq operator would be applied
                    column_name = None
                    break
                if token in mapper.relationships:
                    # follow the relation and change current obj_class and mapper
                    obj_class = mapper.relationships[token].mapper.class_
                    mapper = mapper.relationships[token].mapper
                    column_alias, is_new_alias = self.next_alias(relationships['aliases'], mapper.local_table.name,
                                                                 obj_class)
                    join_chain.append(token)
                    join_chain_ext.append((column_alias, token))
                    continue
                if token not in mapper.column_attrs:
                    # if token is not an op or relation it has to be a valid column
                    raise HTTPBadRequest('Invalid attribute', 'Filter attribute {} is invalid'.format(arg))
                column_name = getattr(obj_class if column_alias is None else column_alias, token)
                """:type column: sqlalchemy.schema.Column"""
                column = mapper.columns[token]
            if column_name is not None:
                # if last token was a column, not an op, assume it's equality
                # if it was a relation it's just going to be ignored
                value = self.deserialize_column(column, value)
                expressions.append(operators.eq(column_name, value))
            # reset everything back to main object
            column_name = None
            obj_class = self.objects_class
            mapper = inspect(obj_class)
            column_alias = None
            if join_chain:
                relationships['join_chains'].append((join_chain, join_chain_ext))
                join_chain = []
                join_chain_ext = []
        result = None
        if len(expressions) > 1:
            result = default_op(*expressions) if default_op != not_ else and_(*expressions)
        elif len(expressions) == 1:
            result = expressions[0]
        return result if default_op != not_ else not_(result)

    @staticmethod
    def next_alias(aliases, name, obj_class, use_existing=True):
        is_new = True
        if name in aliases:
            if use_existing:
                is_new = False
            else:
                aliases[name]['number'] += 1
                aliases[name]['aliased'].append(aliased(obj_class, name=name + '_' + str(aliases[name]['number'])))
        else:
            aliases[name] = {'number': 1,
                             'aliased': [aliased(obj_class, name=name + '_1')]}
        return aliases[name]['aliased'][-1], is_new

    def order_by(self, query, *args):
        """
        :param query: SQLAlchemy Query object
        :type query: sqlalchemy.orm.query.Query

        :return: modified query
        :rtype: sqlalchemy.orm.query.Query
        """
        column = None
        column_name = None
        obj_class = self.objects_class
        mapper = inspect(obj_class)

        for arg in args:
            is_ascending = True
            if len(arg) and arg[0] == '+' or arg[0] == '-':
                is_ascending = arg[:1] == '+'
                arg = arg[1:]
            for token in arg.split('__'):
                if token in mapper.relationships:
                    # follow the relation and change current obj_class and mapper
                    obj_class = mapper.relationships[token].mapper.class_
                    mapper = mapper.relationships[token].mapper
                    query = query.distinct().join(token, aliased=True, from_joinpoint=True)
                    continue
                if token not in mapper.column_attrs:
                    # if token is not an op or relation it has to be a valid column
                    raise HTTPBadRequest('Invalid attribute', 'Order attribute {} is invalid'.format(arg))
                column_name = getattr(obj_class, token, None)
                """:type column: sqlalchemy.schema.Column"""
                column = mapper.columns[token]
            if column_name is not None:
                # if last token was a relation it's just going to be ignored
                query = query.order_by(column if is_ascending else column.desc())
            query = query.reset_joinpoint()
            # reset everything back to main object
            column_name = None
            obj_class = self.objects_class
            mapper = inspect(obj_class)
        return query

    def clean_relations(self, relations):
        """
        Checks all special values in relations and makes sure to always return either a list or None.

        :param relations: relation names
        :type relations: str | list

        :return: either a list (may be empty) or None if all relations should be included
        :rtype: list | None
        """
        if relations == '':
            return []
        elif relations == self.PARAM_RELATIONS_ALL:
            return None
        elif isinstance(relations, str):
            return [relations]

    @staticmethod
    def save_resource(obj, data, db_session):
        """
        Extracts relation dicts from data, saves them and then updates the main object.

        :param obj: a new or existing model
        :type obj: object

        :param data: data to assign to the model and/or its relations
        :type data: dict

        :param db_session: SQLAlchemy session
        :type db_session: sqlalchemy.orm.session.Session
        """
        mapper = inspect(obj).mapper
        attributes = {}
        # first save all relations to avoid assigning dicts as model attributes
        for key, value in data.items():
            if key not in mapper.relationships:
                attributes[key] = value
                continue
            related_mapper = mapper.relationships[key].mapper
            pk = related_mapper.primary_key[0].name
            if isinstance(value, list):
                keys = []
                objects = getattr(obj, key)
                reindexed = {getattr(related, pk): index for index, related in enumerate(objects)}
                for item in value:
                    if isinstance(item, dict):
                        if pk in item and item[pk] in reindexed:
                            AlchemyMixin.save_resource(objects[reindexed[item[pk]]], item, db_session)
                            reindexed.pop(item[pk])
                        else:
                            objects.append(AlchemyMixin.update_or_create(db_session, related_mapper, item))
                    else:
                        if item in reindexed:
                            reindexed.pop(item)
                        else:
                            keys.append(item)
                for index in reindexed.values():
                    del objects[index]
                if keys:
                    expression = related_mapper.primary_key[0].in_(keys)
                    objects += db_session.query(related_mapper.class_).filter(expression).all()
            else:
                rel_obj = getattr(obj, key)
                if isinstance(value, dict):
                    if pk not in value or rel_obj is None:
                        setattr(obj, key, AlchemyMixin.update_or_create(db_session, related_mapper, value))
                    else:
                        AlchemyMixin.save_resource(rel_obj, value, db_session)
                elif rel_obj is None or getattr(rel_obj, pk) != value:
                    expression = related_mapper.primary_key[0].__eq__(value)
                    setattr(obj, key, db_session.query(related_mapper.class_).filter(expression).first())
        # now save the main object
        for key, value in attributes.items():
            if getattr(obj, key) != value:
                setattr(obj, key, value)
        db_session.add(obj)
        return obj

    @staticmethod
    def update_or_create(db_session, mapper, attributes):
        """
        Updated the record if attributes contain the primary key value(s) and creates it if they don't.

        :param db_session:
        :type db_session: sqlalchemy.orm.session.Session

        :param mapper:
        :type mapper: sqlalchemy.orm.mapper.Mapper

        :param attributes:
        :type attributes: dict

        :return:
        :rtype: object
        """
        query_attrs = {}
        for key in mapper.primary_key:
            if key in attributes:
                query_attrs[key] = attributes.pop(key)
        if query_attrs:
            obj = db_session.query(mapper.class_).get(query_attrs[0] if len(query_attrs) == 1 else tuple(query_attrs))
        else:
            obj = mapper.class_()
        if attributes:
            return AlchemyMixin.save_resource(obj, attributes, db_session)
        return obj

    @staticmethod
    def get_or_create(db_session, model_class, query_attrs, update_attrs=None, update_existing=False):
        """
        Fetches the record and if it doesn't exist yet, creates it, handling a race condition.

        :param db_session: session within DB connection
        :type db_session: sqlalchemy.orm.session.Session

        :param model_class: class of the model to return or create
        :type model_class: class

        :param query_attrs: attributes used to fetch the model
        :type query_attrs: dict

        :param update_attrs: attributes used to create a new model
        :type update_attrs: dict

        :param update_existing: if True and update_attrs are set, updates existing records
        :type update_existing: bool

        :return: existing or new object and a flag if existing or new object is being returned
        :rtype: tuple
        """
        query = db_session.query(model_class).filter_by(**query_attrs)
        existing = query.one_or_none()

        if existing:
            if update_existing and update_attrs is not None:
                for key, value in update_attrs.items():
                    if getattr(existing, key) != value:
                        setattr(existing, key, value)
            return existing, False

        db_session.begin_nested()
        try:
            if update_attrs is None:
                update_attrs = query_attrs
            else:
                update_attrs.update(query_attrs)
            new_object = model_class(**update_attrs)
            db_session.add(new_object)
            db_session.commit()
        except IntegrityError:
            db_session.rollback()
            existing = query.one_or_none()
            if update_existing and update_attrs is not None:
                for key, value in update_attrs.items():
                    if getattr(existing, key) != value:
                        setattr(existing, key, value)
            return existing, False
        return new_object, True

    @staticmethod
    def get_default_schema(model_class, method='POST'):
        """
        Returns a schema to be used in falconjsonio.schema.request_schema decorator
        :return:
        """
        schema = {
            'type': 'object',
            'properties': {
            },
        }
        if method == 'POST':
            schema['required'] = []
        return schema


class CollectionResource(AlchemyMixin, BaseCollectionResource):
    """
    Allows to fetch a collection of a resource (GET) and to create new resource in that collection (POST).
    May be extended to allow batch operations (ex. PATCH).
    When fetching a collection (GET), following params are supported:
    * limit, offset - for pagination
    * total_count - to calculate total number of items matching filters, without pagination
    * relations - list of relation names to include in the result, uses special value `_all` for all relations
    * all other params are treated as filters, syntax mimics Django filters, see `AlchemyMixin._underscore_operators`
    User input can be validated by attaching the `falconjsonio.schema.request_schema()` decorator.
    """
    VIOLATION_UNIQUE = '23505'

    def __init__(self, objects_class, db_engine, max_limit=None):
        """
        :param objects_class: class represent single element of object lists that suppose to be returned
        :param db_engine: SQL Alchemy engine
        :type db_engine: sqlalchemy.engine.Engine
        """
        super(CollectionResource, self).__init__(objects_class, max_limit)
        self.db_engine = db_engine
        if not hasattr(self, '__request_schemas__'):
            self.__request_schemas__ = {}
        self.__request_schemas__['POST'] = AlchemyMixin.get_default_schema(objects_class, 'POST')

    def get_queryset(self, req, resp, db_session=None):
        query = db_session.query(self.objects_class)
        relations = self.clean_relations(self.get_param_or_post(req, self.PARAM_RELATIONS, ''))
        if relations is None or len(relations):
            query = query.options(subqueryload('*') if relations is None else subqueryload(*relations))
        order = self.get_param_or_post(req, self.PARAM_ORDER)
        if order:
            if not isinstance(order, list):
                order = [order]
            query = self.order_by(query, *order)
        else:
            primary_keys = inspect(self.objects_class).primary_key
            query = query.order_by(*primary_keys)
        if self.PARAM_SEARCH in req.params:
            try:
                req.params.update(json.loads(req.params.pop(self.PARAM_SEARCH)))
            except ValueError:
                raise HTTPBadRequest('Invalid attribute',
                                     'Value of {} filter attribute is invalid'.format(self.PARAM_SEARCH))
        return self.filter_by(query, req.params)

    def get_total_objects(self, queryset):
        primary_keys = inspect(self.objects_class).primary_key
        count_q = queryset.statement.with_only_columns([func.count(*primary_keys)]).order_by(None)
        return queryset.session.execute(count_q).scalar()

    def get_object_list(self, queryset, limit=None, offset=None):
        if limit is None:
            limit = self.max_limit
        if offset is None:
            offset = 0
        if limit is not None:
            if self.max_limit is not None:
                limit = min(limit, self.max_limit)
            limit = max(limit, 0)
            queryset = queryset.limit(limit)
        offset = max(offset, 0)
        return queryset.offset(offset)

    def on_get(self, req, resp):
        limit = self.get_param_or_post(req, self.PARAM_LIMIT)
        offset = self.get_param_or_post(req, self.PARAM_OFFSET)
        if limit is not None:
            limit = int(limit)
        if offset is not None:
            offset = int(offset)
        get_total = self.get_param_or_post(req, self.PARAM_TOTAL_COUNT)
        # retrieve that param without removing it so self.get_queryset() so it can also use it
        relations = self.clean_relations(req.params.get(self.PARAM_RELATIONS, ''))

        with session_scope(self.db_engine) as db_session:
            query = self.get_queryset(req, resp, db_session)
            total = self.get_total_objects(query) if get_total else None

            object_list = self.get_object_list(query, limit, offset)

            serialized = [self.serialize(obj, relations_include=relations) for obj in object_list]
            result = {
                'results': serialized,
                'total': total,
                'returned': len(serialized),  # avoid calling object_list.count() which executes the query again
            }

        self.render_response(result, req, resp)

    def create(self, req, resp, data):
        relations = self.clean_relations(self.get_param_or_post(req, self.PARAM_RELATIONS, ''))
        try:
            with session_scope(self.db_engine) as db_session:
                resource = self.save_resource(self.objects_class(), data, db_session)
                db_session.commit()
                return self.serialize(resource, relations_include=relations)
        except (IntegrityError, ProgrammingError) as err:
            # Cases such as unallowed NULL value should have been checked before we got here (e.g. validate against
            # schema using falconjsonio) - therefore assume this is a UNIQUE constraint violation
            if isinstance(err, IntegrityError) or err.orig.args[1] == self.VIOLATION_UNIQUE:
                raise HTTPConflict('Conflict', 'Unique constraint violated')
            else:
                raise


class SingleResource(AlchemyMixin, BaseSingleResource):
    """
    Allows to fetch a single resource (GET) and to update (PATCH, PUT) or remove it (DELETE).
    When fetching a resource (GET), following params are supported:
    * relations - list of relation names to include in the result, uses special value `_all` for all relations
    User input can be validated by attaching the `falconjsonio.schema.request_schema()` decorator.
    """
    VIOLATION_FOREIGN_KEY = '23503'

    def __init__(self, objects_class, db_engine):
        """
        :param objects_class: class represent single element of object lists that suppose to be returned
        :param db_engine: SQL Alchemy engine
        :type db_engine: sqlalchemy.engine.Engine
        """
        super(SingleResource, self).__init__(objects_class)
        self.db_engine = db_engine
        if not hasattr(self, '__request_schemas__'):
            self.__request_schemas__ = {}
        self.__request_schemas__['POST'] = AlchemyMixin.get_default_schema(objects_class, 'POST')
        self.__request_schemas__['PUT'] = AlchemyMixin.get_default_schema(objects_class, 'POST')

    def get_object(self, req, resp, path_params, db_session=None):
        query = db_session.query(self.objects_class)

        for key, value in path_params.items():
            attr = getattr(self.objects_class, key, None)
            query = query.filter(attr == value)

        conditions = dict(req.params)
        if self.PARAM_RELATIONS in conditions:
            conditions.pop(self.PARAM_RELATIONS)
        query = self.filter_by(query, conditions)

        try:
            obj = query.one()
        except NoResultFound:
            raise HTTPNotFound()
        except MultipleResultsFound:
            raise HTTPBadRequest('Multiple results', 'Query params match multiple records')
        return obj

    def on_get(self, req, resp, *args, **kwargs):
        relations = self.clean_relations(self.get_param_or_post(req, self.PARAM_RELATIONS, ''))
        with session_scope(self.db_engine) as db_session:
            obj = self.get_object(req, resp, kwargs, db_session)

            result = {
                'results': self.serialize(obj, relations_include=relations),
            }

        self.render_response(result, req, resp)

    def on_delete(self, req, resp, *args, **kwargs):
        try:
            with session_scope(self.db_engine) as db_session:
                obj = self.get_object(req, resp, kwargs, db_session)

                self.delete(req, resp, obj)
        except (IntegrityError, ProgrammingError) as err:
            # This should only be caused by foreign key constraint being violated
            if isinstance(err, IntegrityError) or err.orig.args[1] == self.VIOLATION_FOREIGN_KEY:
                raise HTTPConflict('Conflict', 'Other content links to this')
            else:
                raise

        self.render_response({}, req, resp)

    def update(self, req, resp, data, obj, db_session=None):
        relations = self.clean_relations(self.get_param_or_post(req, self.PARAM_RELATIONS, ''))
        resource = self.save_resource(obj, data, db_session)
        db_session.commit()
        return self.serialize(resource, relations_include=relations)

    def on_put(self, req, resp, *args, **kwargs):
        status_code = falcon.HTTP_OK
        try:
            with session_scope(self.db_engine) as db_session:
                obj = self.get_object(req, resp, kwargs, db_session)

                data = self.deserialize(req.context['doc'] if 'doc' in req.context else None)
                data, errors = self.clean(data)
                if errors:
                    result = {'errors': errors}
                    status_code = falcon.HTTP_BAD_REQUEST
                else:
                    result = self.update(req, resp, data, obj, db_session)
        except (IntegrityError, ProgrammingError) as err:
            # Cases such as unallowed NULL value should have been checked before we got here (e.g. validate against
            # schema using falconjsonio) - therefore assume this is a UNIQUE constraint violation
            if isinstance(err, IntegrityError) or err.orig.args[1] == self.VIOLATION_FOREIGN_KEY:
                raise HTTPConflict('Conflict', 'Unique constraint violated')
            else:
                raise

        self.render_response(result, req, resp, status_code)
