from collections import OrderedDict
from contextlib import contextmanager
from datetime import datetime, time
from decimal import Decimal
from enum import Enum

import collections
from functools import lru_cache

import alchemyjsonschema
import falcon
try:
    import ujson as json
except ImportError:
    import json

from falcon import HTTPConflict, HTTPBadRequest, HTTPNotFound
from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.orm import sessionmaker, subqueryload, aliased
from sqlalchemy.orm.base import MANYTOONE
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.sql import sqltypes, operators, extract, func
from sqlalchemy.sql.expression import and_, or_, not_, desc, select
from sqlalchemy.sql.functions import Function

from falcon_dbapi.resources.base import BaseCollectionResource, BaseSingleResource


def _is_int(s):
    try:
        int(s)
    except ValueError:
        return False
    return True


class AlchemyMixin(object):
    """
    Provides serialize and deserialize methods to convert between JSON and SQLAlchemy datatypes.
    """
    MULTIVALUE_SEPARATOR = ','
    PARAM_RELATIONS = 'relations'
    PARAM_RELATIONS_ALL = '_all'
    DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
    RELATIONS_AS_LIST = True
    IGNORE_UNKNOWN_FILTER = False

    _underscore_operators = {
        'exact':        operators.eq,
        'notexact':     operators.ne,
        'gt':           operators.gt,
        'lt':           operators.lt,
        'gte':          operators.ge,
        'lte':          operators.le,
        'range':        operators.between_op,
        'notrange':     operators.notbetween_op,
        'in':           operators.in_op,
        'notin':        operators.notin_op,
        'contains':     operators.contains_op,
        'notcontains':  operators.notcontains_op,
        'icontains': lambda c, x: c.ilike('%' + x.replace('%', '\%').replace('_', '\_') + '%'),
        'noticontains': lambda c, x: c.notilike('%' + x.replace('%', '\%').replace('_', '\_') + '%'),
        'match':        operators.match_op,
        'notmatch':     operators.notmatch_op,
        'iexact':       operators.ilike_op,
        'notiexact':    operators.notilike_op,
        'startswith':   operators.startswith_op,
        'notstartswith': operators.notstartswith_op,
        'endswith':     operators.endswith_op,
        'notendswith':  operators.notendswith_op,
        'hasall': lambda c, x: c.has_all(x),
        'hasany': lambda c, x: c.has_any(x),
        'haskey': lambda c, x: c.has_key(x),  # noqa
        'overlap': lambda c, x: c.op('&&')(x),
        'istartswith': lambda c, x: c.ilike(x.replace('%', '\%').replace('_', '\_') + '%'),
        'notistartswith': lambda c, x: c.notilike(x.replace('%', '\%').replace('_', '\_') + '%'),
        'iendswith': lambda c, x: c.ilike('%' + x.replace('%', '\%').replace('_', '\_')),
        'notiendswith': lambda c, x: c.notilike('%' + x.replace('%', '\%').replace('_', '\_')),
        'isnull': lambda c, x: c.is_(None) if x else c.isnot(None),
        'isnotnull': lambda c, x: c.isnot(None) if x else c.is_(None),
        'year': lambda c, x: extract('year', c) == x,
        'month': lambda c, x: extract('month', c) == x,
        'day': lambda c, x: extract('day', c) == x,
        'func': Function,
        'sfunc': Function,
        'efunc': Function,
    }
    _logical_operators = {
        'or': or_,
        'and': and_,
        'not': not_,
    }

    @classmethod
    @contextmanager
    def session_scope(cls, db_engine=None, session_class=None):
        """
        Provide a scoped db session for a series of operarions.
        The session is created immediately before the scope begins, and is closed
        on scope exit.
        :param db_engine: SQLAlchemy Engine or other Connectable
        :type db_engine: sqlalchemy.engine.Connectable

        :param session_class: SQLAlchemy Session
        :type session_class: sqlalchemy.orm.Session
        """
        if session_class is None:
            session_class = sessionmaker(bind=db_engine)
        db_session = session_class()
        try:
            yield db_session
            db_session.commit()
        except:
            db_session.rollback()
            raise
        finally:
            db_session.close()

    @classmethod
    def serialize(cls, obj, skip_primary_key=False, skip_foreign_keys=False, relations_level=1, relations_ignore=None,
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
        data = cls.serialize_columns(obj, data, skip_primary_key, skip_foreign_keys)
        if relations_level > 0:
            if relations_ignore is None:
                relations_ignore = []
            data = cls.serialize_relations(obj, data, relations_level, relations_ignore, relations_include)
        return data

    @classmethod
    def serialize_columns(cls, obj, data, skip_primary_key=False, skip_foreign_keys=False):
        columns = inspect(obj).mapper.columns
        for key, column in columns.items():
            if skip_primary_key and column.primary_key:
                continue
            if skip_foreign_keys and len(column.foreign_keys):
                continue
            if isinstance(column.type, TSVECTOR):
                continue
            data[key] = cls.serialize_column(column, getattr(obj, key))

        return data

    @classmethod
    def serialize_column(cls, column, value):
        if isinstance(value, datetime):
            return value.strftime(cls.DATETIME_FORMAT)
        elif isinstance(value, time):
            return value.isoformat()
        elif isinstance(value, Decimal):
            return float(value)
        elif isinstance(value, Enum):
            return value.value
        return value

    @classmethod
    def serialize_relations(cls, obj, data, relations_level=1, relations_ignore=None, relations_include=None):
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
                data[relation.key] = cls.serialize(rel_obj,
                                                   relations_level=relations_level - 1,
                                                   relations_ignore=relations_ignore)
            elif not relation.uselist:
                data.update(cls.serialize(rel_obj,
                                          skip_primary_key=True,
                                          relations_level=relations_level - 1,
                                          relations_ignore=relations_ignore))
            else:
                if cls.RELATIONS_AS_LIST:
                    data[relation.key] = [
                        cls.serialize(rel,
                                      skip_primary_key=False,
                                      relations_level=relations_level - 1,
                                      relations_ignore=relations_ignore)
                        for rel in rel_obj
                    ]
                else:
                    data[relation.key] = {
                        str(rel.id): cls.serialize(rel,
                                                   skip_primary_key=True,
                                                   relations_level=relations_level - 1,
                                                   relations_ignore=relations_ignore)
                        for rel in rel_obj if hasattr(rel, 'id')
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

        if mapper is None:
            mapper = inspect(self.objects_class)
        for key, value in data.items():
            if key in mapper.relationships:
                attributes[key] = self.deserialize_relation(mapper.relationships[key].mapper, value)
            elif key in mapper.columns:
                attributes[key] = self.deserialize_column(mapper.columns[key], value)
            else:
                attributes.update(self.deserialize_onetoone(mapper, key, value))

        return attributes

    def deserialize_column(self, column, value):
        if value is None:
            return None
        if isinstance(column.type, sqltypes.DateTime):
            return datetime.strptime(value, self.DATETIME_FORMAT)
        if isinstance(column.type, sqltypes.Time):
            hour, minute, second = value.split(':')
            return time(int(hour), int(minute), int(second))
        if isinstance(column.type, sqltypes.Integer):
            return int(value)
        if isinstance(column.type, sqltypes.Float):
            return float(value)
        return value

    def deserialize_relation(self, rel_mapper, value):
        # handle a special case, when value is a dict with only all integer keys, then convert it to a list
        if isinstance(value, dict) and all(_is_int(pk) for pk in value.keys()):
            replacement = []
            for pk, attrs in value.items():
                attrs[rel_mapper.primary_key[0].name] = pk
                replacement.append(attrs)
            value = replacement

        if isinstance(value, dict):
            return self.deserialize(value, rel_mapper)
        elif isinstance(value, list):
            result = []
            for item in value:
                if isinstance(item, dict):
                    result.append(self.deserialize(item, rel_mapper))
                else:
                    result.append(item)
            return result
        return value

    def deserialize_onetoone(self, mapper, key, value):
        result = {}
        for relation in mapper.relationships:
            if relation.direction == MANYTOONE or relation.uselist or key not in relation.mapper.columns:
                continue
            result[key] = self.deserialize_column(relation.mapper.columns[key], value)
        return result

    @lru_cache(maxsize=None)
    def get_schema(self, objects_class):
        extended_mapping = alchemyjsonschema.default_column_to_schema.copy()
        extended_mapping[sqltypes.ARRAY] = 'array'
        extended_mapping[sqltypes.JSON] = 'object'
        extended_mapping[TSVECTOR] = 'array'
        factory = alchemyjsonschema.SchemaFactory(alchemyjsonschema.StructuralWalker,
                                                  classifier=alchemyjsonschema.Classifier(extended_mapping))
        return factory(objects_class, depth=1)

    def filter_by(self, query, conditions, order_criteria=None):
        """
        :param query: SQLAlchemy Query object
        :type query: sqlalchemy.orm.query.Query

        :param conditions: conditions dictionary
        :type conditions: dict

        :param order_criteria: optional order criteria
        :type order_criteria: dict

        :return: modified query
        :rtype: sqlalchemy.orm.query.Query
        """
        return self._filter_or_exclude(query, conditions, order_criteria=order_criteria)

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

    def _filter_or_exclude(self, query, conditions, default_op=None, order_criteria=None):
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
        order_expressions = []
        if order_criteria:
            order_expressions = self._build_order_expressions(order_criteria, relationships)
        query = self._apply_joins(query, relationships, distinct=expressions is not None)
        if expressions is not None:
            query = query.filter(expressions)
        if order_criteria and order_expressions is not None:
            query = query.order_by(*order_expressions)
        return query

    def _apply_joins(self, query, relationships, distinct=True):
        longest_chains = []
        for chain_a, chain_a_ext, chain_a_is_outer in relationships['join_chains']:
            is_longest = True
            any_is_outer = chain_a_is_outer
            for chain_b, chain_b_ext, chain_b_is_outer in relationships['join_chains']:
                if chain_a == chain_b:
                    if chain_b_is_outer:
                        any_is_outer = True
                    continue
                if set(chain_a).issubset(chain_b):
                    is_longest = False
                    break
            if is_longest and (chain_a_ext, any_is_outer) not in longest_chains:
                longest_chains.append((chain_a_ext, any_is_outer))
        if not longest_chains:
            return query
        for chain, chain_is_outer in longest_chains:
            for alias, relation in chain:
                query = query.join((alias, relation), from_joinpoint=True, isouter=chain_is_outer)
            query = query.reset_joinpoint()
        return query.distinct() if distinct else query

    def _build_filter_expressions(self, conditions, default_op, relationships):
        """
        :param conditions: conditions dictionary
        :type conditions: dict

        :param default_op: a default operator to join all filter expressions
        :type default_op: function

        :param relationships:  a dict with all joins to apply, describes current state in recurrent calls
        :type relationships: dict

        :return: expressions list
        :rtype: list
        """
        if default_op is None:
            default_op = and_

        expressions = []

        for arg, value in conditions.items():
            if arg in self._logical_operators:
                expression = self._parse_logical_op(arg, value, self._logical_operators[arg], relationships)
            else:
                expression = self._parse_tokens(self.objects_class, arg.split('__'), value, relationships,
                                                lambda c, n, v: operators.eq(n, self.deserialize_column(c, v)))
            if expression is not None:
                expressions.append(expression)
        result = None
        if len(expressions) > 1:
            result = default_op(*expressions) if default_op != not_ else not_(and_(*expressions))
        elif len(expressions) == 1:
            result = expressions[0] if default_op != not_ else not_(expressions[0])
        return result

    def _parse_logical_op(self, arg, value, default_op, relationships):
        """
        :param arg: condition name
        :type arg: str

        :param value: condition value
        :type value: dict | list

        :param default_op: a default operator to join all filter expressions
        :type default_op: function

        :param relationships:  a dict with all joins to apply, describes current state in recurrent calls
        :type relationships: dict

        :return: expressions list
        :rtype: list
        """
        if isinstance(value, dict):
            return self._build_filter_expressions(value, default_op, relationships)
        if not isinstance(value, list):
            raise HTTPBadRequest('Invalid attribute', 'Filter attribute {} is invalid'.format(arg))
        expressions = []
        for subconditions in value:
            if not isinstance(subconditions, dict):
                raise HTTPBadRequest('Invalid attribute', 'Filter attribute {} is invalid'.format(arg))
            subexpressions = self._build_filter_expressions(subconditions, and_, relationships)
            if subexpressions is not None:
                expressions.append(subexpressions)
        result = None
        if len(expressions) > 1:
            result = default_op(*expressions) if default_op != not_ else not_(and_(*expressions))
        elif len(expressions) == 1:
            result = expressions[0] if default_op != not_ else not_(expressions[0])
        return result

    def _build_expression(self, tokens, index, column_name, value, obj_class, relationships):
        token = tokens[index]
        op = self._underscore_operators[token]

        if op != Function:
            return op(column_name, value)

        expression = column_name
        if len(tokens[index+1:]) > 1:
            if token == 'efunc':
                value = self._parse_tokens(obj_class, tokens[index+1:-1], value, relationships,
                                           lambda c, n, v: n)
            else:
                for func_name in tokens[index+1:-1]:
                    expression = Function(func_name, expression)

        if tokens[-1] in self._underscore_operators:
            return self._underscore_operators[tokens[-1]](expression, value)

        if token == 'sfunc':
            return Function(tokens[-1], expression)

        return Function(tokens[-1], expression, value)

    def _parse_tokens(self, obj_class, tokens, value, relationships, default_expression=None):
        column_name = None
        column = None
        column_alias = obj_class
        mapper = inspect(obj_class)
        join_chain = []
        join_chain_ext = []
        join_is_outer = False
        for index, token in enumerate(tokens):
            if token == CollectionResource.PARAM_TEXT_QUERY:
                query_method = getattr(obj_class, 'get_term_query', None)
                if not callable(query_method):
                    raise HTTPBadRequest('Invalid attribute', 'Param {} is invalid, specific object '
                                                              'can\'t provide a query'.format('__'.join(tokens)))

                return query_method(self=obj_class, column_alias=column_alias, column_name=column_name, value=value,
                                    default_op=or_ if tokens[-1] == 'or' else and_)

            if column_name is not None and token in self._underscore_operators:
                op = self._underscore_operators[token]
                if op in [operators.between_op, operators.in_op]:
                    if not isinstance(value, list):
                        value = [value]
                # isnull is the only operator where the value is not of the same type as the column
                if token != 'isnull' and token != 'isnotnull':
                    if isinstance(value, list):
                        value = list(map(lambda x: self.deserialize_column(column, x), value))
                    else:
                        value = self.deserialize_column(column, value)

                expression = self._build_expression(tokens, index, column_name, value, obj_class, relationships)

                if token == 'isnull':
                    join_is_outer = True
                if join_chain:
                    relationships['join_chains'].append((join_chain, join_chain_ext, join_is_outer))
                return expression

            if token in mapper.relationships:
                # follow the relation and change current obj_class and mapper
                obj_class = mapper.relationships[token].mapper.class_
                mapper = mapper.relationships[token].mapper
                column_alias, is_new_alias = self.next_alias(relationships['aliases'], token, obj_class,
                                                             prefix=relationships.get('prefix', ''))
                join_chain.append(token)
                join_chain_ext.append((column_alias, token))
                continue

            if token not in mapper.column_attrs:
                if self.IGNORE_UNKNOWN_FILTER:
                    return None
                # if token is not an op or relation it has to be a valid column
                raise HTTPBadRequest('Invalid attribute', 'Param {} is invalid, part {} is expected '
                                                          'to be a known column name'.format('__'.join(tokens), token))

            column_name = getattr(column_alias, token)
            """:type column: sqlalchemy.schema.Column"""
            column = mapper.columns[token]

        if join_chain:
            relationships['join_chains'].append((join_chain, join_chain_ext, join_is_outer))

        if column_name is not None and default_expression is not None:
            # if last token was a relation it's just going to be ignored
            return default_expression(column, column_name, value)

        return None

    @staticmethod
    def get_tsquery(value, default_op):
        if isinstance(value, list):
            tq = func.plainto_tsquery('english', value.pop())
            while len(value):
                tq = tq.op('||' if default_op == or_ else '&&')(func.plainto_tsquery('english', value.pop()))
        else:
            tq = func.plainto_tsquery('english', value)
        return tq

    @staticmethod
    def next_alias(aliases, name, obj_class, use_existing=True, prefix=''):
        is_new = True
        if name in aliases:
            if use_existing:
                is_new = False
            else:
                aliases[name]['number'] += 1
                aliases[name]['aliased'].append(
                    aliased(obj_class, name=prefix + name + '_' + str(aliases[name]['number'])))
        else:
            aliases[name] = {'number': 1,
                             'aliased': [aliased(obj_class, name=prefix + name + '_1')]}
        return aliases[name]['aliased'][-1], is_new

    def order_by(self, query, criteria):
        """
        :param query: SQLAlchemy Query object
        :type query: sqlalchemy.orm.query.Query

        :return: modified query
        :rtype: sqlalchemy.orm.query.Query
        """
        relationships = {
            'aliases': {},
            'join_chains': [],
        }
        expressions = self._build_order_expressions(criteria, relationships)
        query = self._apply_joins(query, relationships, distinct=False)
        if expressions is not None:
            query = query.order_by(*expressions)
        return query

    def _build_order_expressions(self, criteria, relationships):
        """
        :param criteria: criteria dictionary
        :type criteria: dict

        :param relationships:  a dict with all joins to apply, describes current state in recurrent calls
        :type relationships: dict

        :return: expressions list
        :rtype: list
        """
        expressions = []

        if isinstance(criteria, dict):
            criteria = list(criteria.items())
        for arg in criteria:
            if isinstance(arg, tuple):
                arg, value = arg
            else:
                value = None
            is_ascending = True
            if len(arg) and arg[0] == '+' or arg[0] == '-':
                is_ascending = arg[:1] == '+'
                arg = arg[1:]
            expression = self._parse_tokens(self.objects_class, arg.split('__'), value, relationships,
                                            lambda c, n, v: n)
            if expression is not None:
                expressions.append(expression if is_ascending else desc(expression))
        return expressions

    def clean_relations(self, relations):
        """
        Checks all special values in relations and makes sure to always return either a list or None.

        :param relations: relation names
        :type relations: str | list

        :return: either a list (may be empty) or None if all relations should be included
        :rtype: list[str] | None
        """
        if relations == '':
            return []
        elif relations == self.PARAM_RELATIONS_ALL:
            return None
        elif isinstance(relations, str):
            return [relations]
        return relations

    @classmethod
    def save_resource(cls, obj, data, db_session):
        """
        Extracts relation dicts from data, saves them and then updates the main object.

        :param obj: a new or existing model
        :type obj: object

        :param data: data to assign to the model and/or its relations
        :type data: dict

        :param db_session: SQLAlchemy session
        :type db_session: sqlalchemy.orm.session.Session
        """
        # fetching related objects should not trigger saving of main object,
        # because FKs could not have been set yet
        autoflush = db_session.autoflush
        db_session.autoflush = False
        mapper = inspect(obj).mapper
        for key, value in data.items():
            if key in mapper.relationships or getattr(obj, key) == value:
                continue
            setattr(obj, key, value)
        db_session.add(obj)
        cls.save_resource_relations(obj, data, db_session)
        db_session.autoflush = autoflush
        return obj

    @classmethod
    def save_resource_relations(cls, obj, data, db_session):
        mapper = inspect(obj).mapper
        for key, value in data.items():
            if key not in mapper.relationships:
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
                            cls.save_resource(objects[reindexed[item[pk]]], item, db_session)
                            reindexed.pop(item[pk])
                        else:
                            objects.append(cls.update_or_create(db_session, related_mapper, item))
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
                    relationship = mapper.relationships[key]
                    if (relationship.direction == MANYTOONE or not relationship.uselist) \
                            and (pk not in value or rel_obj is None):
                        setattr(obj, key, cls.update_or_create(db_session, related_mapper, value))
                    else:
                        cls.save_resource(rel_obj, value, db_session)
                elif rel_obj is None or getattr(rel_obj, pk) != value:
                    expression = related_mapper.primary_key[0].__eq__(value)
                    setattr(obj, key, db_session.query(related_mapper.class_).filter(expression).first())

    @classmethod
    def update_or_create(cls, db_session, mapper, attributes):
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
            return cls.save_resource(obj, attributes, db_session)
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

    def __init__(self, objects_class, db_engine, max_limit=None, eager_limit=None):
        """
        :param objects_class: class represent single element of object lists that suppose to be returned

        :param db_engine: SQL Alchemy engine
        :type db_engine: sqlalchemy.engine.Engine

        :param max_limit: max limit of elements that suppose to be returned by default
        :type max_limit: int

        :param eager_limit: if None or the value of limit param is greater than this, subquery eager loading
                            will be enabled
        :type eager_limit: int
        """
        super(CollectionResource, self).__init__(objects_class, max_limit)
        self.db_engine = db_engine
        self.eager_limit = eager_limit
        if not hasattr(self, '__request_schemas__'):
            self.__request_schemas__ = {}
        self.__request_schemas__['POST'] = AlchemyMixin.get_default_schema(objects_class, 'POST')

    def get_eager_queryset(self, req, resp, db_session=None, limit=None):
        """
        Return a default query with eager options set if any relations has been requested.

        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response

        :param db_session: SQLAlchemy session
        :type db_session: sqlalchemy.orm.session.Session

        :param limit: max number of records fetched
        :type limit: int | None

        :return: a query from `object_class`
        """
        query = db_session.query(self.objects_class)
        relations = self.clean_relations(self.get_param_or_post(req, self.PARAM_RELATIONS, ''))
        if self.eager_limit is not None and (limit is None or limit <= self.eager_limit):
            return query
        if relations is None:
            query = query.options(subqueryload('*'))
        elif len(relations):
            for relation in relations:
                query = query.options(subqueryload(relation))
        return query

    def get_special_params(self):
        return [self.PARAM_LIMIT, self.PARAM_OFFSET, self.PARAM_TOTAL_COUNT, self.PARAM_TOTALS, self.PARAM_TEXT_QUERY,
                self.PARAM_RELATIONS]

    def get_queryset(self, req, resp, db_session=None, limit=None):
        """
        Return a query object used to fetch data.

        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response

        :param db_session: SQLAlchemy session
        :type db_session: sqlalchemy.orm.session.Session

        :param limit: max number of records fetched
        :type limit: int | None

        :return: a query from `object_class`
        """
        query = self.get_eager_queryset(req, resp, db_session, limit)
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
            primary_keys = inspect(self.objects_class).primary_key
            return self.filter_by(query, conditions).order_by(*primary_keys)

        if isinstance(order, str):
            if (order[0] == '{' and order[-1] == '}') or (order[0] == '[' and order[-1] == ']'):
                try:
                    order = json.loads(order)
                except ValueError:
                    # not valid json, ignore and try to parse as an ordinary list of attributes
                    pass
        if not isinstance(order, list) and not isinstance(order, dict):
            order = [order]
        return self.filter_by(query, conditions, order)

    def get_total_objects(self, queryset, totals):
        if not totals:
            return {}
        agg_query, dimensions = self._build_total_expressions(queryset, totals)

        def nested_dict(n, type):
            """Creates an n-dimension dictionary where the n-th dimension is of type 'type'
            """
            if n <= 1:
                return type()
            return collections.defaultdict(lambda: nested_dict(n - 1, type))

        result = nested_dict(len(dimensions) + 2, None)
        for aggs in queryset.session.execute(agg_query):
            for metric_key, metric_value in aggs.items():
                if metric_key in dimensions:
                    continue
                last_result = result
                last_key = 'total_' + metric_key
                for dimension in dimensions:
                    last_result = last_result[last_key]
                    last_key = str(aggs[dimension])
                last_result[last_key] = metric_value if not isinstance(metric_value, Decimal) else float(metric_value)
        return result

    def _build_total_expressions(self, queryset, totals):
        mapper = inspect(self.objects_class)
        primary_keys = mapper.primary_key
        relationships = {
            'aliases': {},
            'join_chains': [],
            'prefix': 'totals_',
        }
        aggregates = []
        group_cols = OrderedDict()
        group_by = []
        group_limit = None
        for total in totals:
            for aggregate, columns in total.items():
                if aggregate == self.AGGR_GROUPLIMIT:
                    if not isinstance(columns, int):
                        raise HTTPBadRequest('Invalid attribute', 'Group limit option requires an integer value')
                    group_limit = columns
                    continue
                if not columns:
                    if aggregate == self.AGGR_GROUPBY:
                        raise HTTPBadRequest('Invalid attribute', 'Group by option requires at least one column name')
                    if len(primary_keys) > 1:
                        aggregates.append(Function(aggregate, func.row(*primary_keys)).label(aggregate))
                    else:
                        aggregates.append(Function(aggregate, *primary_keys).label(aggregate))
                    continue
                if not isinstance(columns, list):
                    columns = [columns]
                for column in columns:
                    expression = self._parse_tokens(self.objects_class, column.split('__'), None, relationships,
                                                    lambda c, n, v: n)
                    if expression is not None:
                        if aggregate == self.AGGR_GROUPBY:
                            group_cols[column] = expression.label(column)
                            group_by.append(expression)
                        else:
                            aggregates.append(Function(aggregate, expression).label(aggregate))
        agg_query = self._apply_joins(queryset, relationships, distinct=False)
        group_cols_expr = list(group_cols.values())
        columns = group_cols_expr + aggregates
        if group_limit:
            row_order = list(map(lambda c: c.desc(), aggregates))
            columns.append(func.row_number().over(partition_by=group_cols_expr[:-1],
                                                  order_by=row_order).label('row_number'))
        order = ','.join(list(map(str, range(1, len(group_cols_expr) + 1)))
                         + list(map(lambda c: str(c) + ' DESC', range(1 + len(group_cols_expr),
                                                                      len(aggregates) + len(group_cols_expr) + 1))))
        agg_query = agg_query.statement.with_only_columns(columns).order_by(None).order_by(order)
        if group_by:
            agg_query = agg_query.group_by(*group_by)
        if group_limit:
            subquery = agg_query.alias()
            agg_query = select([subquery]).where(subquery.c.row_number <= group_limit)
        return agg_query, list(group_cols.keys())

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
        totals = self.get_param_totals(req)
        # retrieve that param without removing it so self.get_queryset() so it can also use it
        relations = self.clean_relations(self.get_param_or_post(req, self.PARAM_RELATIONS, '', pop_params=False))

        with self.session_scope(self.db_engine) as db_session:
            query = self.get_queryset(req, resp, db_session, limit)
            totals = self.get_total_objects(query, totals)

            object_list = self.get_object_list(query, limit, offset)

            serialized = [self.serialize(obj, relations_include=relations,
                                         relations_ignore=list(getattr(self, 'serialize_ignore', [])))
                          for obj in object_list]
            result = {'results': serialized,
                      'total': totals['total_count'] if 'total_count' in totals else None,
                      'returned': len(serialized)}  # avoid calling object_list.count() which executes the query again
            result.update(totals)

        headers = {'x-api-total': str(result['total']) if result['total'] is not None else '',
                   'x-api-returned': str(result['returned'])}
        resp.set_headers(headers)
        self.render_response(result, req, resp)

    def on_head(self, req, resp):
        limit = self.get_param_or_post(req, self.PARAM_LIMIT)
        offset = self.get_param_or_post(req, self.PARAM_OFFSET)
        if limit is not None:
            limit = int(limit)
        if offset is not None:
            offset = int(offset)
        totals = self.get_param_totals(req)

        with self.session_scope(self.db_engine) as db_session:
            query = self.get_queryset(req, resp, db_session, limit)
            totals = self.get_total_objects(query, totals)

            object_list = self.get_object_list(query, limit, offset)

            headers = {'x-api-total': str(totals['total_count']) if 'total_count' in totals else '',
                       'x-api-returned': str(len(object_list))}

        resp.set_headers(headers)
        resp.status = falcon.HTTP_NO_CONTENT

    def create(self, req, resp, data, db_session=None):
        """
        Create a new or update an existing record using provided data.
        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response

        :param data:
        :type data: dict

        :param db_session: SQLAlchemy session
        :type db_session: sqlalchemy.orm.session.Session

        :return: created object, serialized to a dict
        :rtype: dict
        """
        relations = self.clean_relations(self.get_param_or_post(req, self.PARAM_RELATIONS, ''))
        resource = self.save_resource(self.objects_class(), data, db_session)
        db_session.commit()
        return self.serialize(resource, relations_include=relations,
                              relations_ignore=list(getattr(self, 'serialize_ignore', [])))

    def on_post(self, req, resp, *args, **kwargs):
        data = self.deserialize(req.context['doc'] if 'doc' in req.context else None)
        data, errors = self.clean(data)
        if errors:
            result = {'errors': errors}
            status_code = falcon.HTTP_BAD_REQUEST
            self.render_response(result, req, resp, status_code)
            return

        try:
            with self.session_scope(self.db_engine) as db_session:
                result = self.create(req, resp, data, db_session=db_session)
        except IntegrityError:
            raise HTTPConflict('Conflict', 'Unique constraint violated')
        except ProgrammingError as err:
            # Cases such as unallowed NULL value should have been checked before we got here (e.g. validate against
            # schema using falconjsonio) - therefore assume this is a UNIQUE constraint violation
            if len(err.orig.args) > 1 and err.orig.args[1] == self.VIOLATION_UNIQUE:
                raise HTTPConflict('Conflict', 'Unique constraint violated')
            raise
        status_code = falcon.HTTP_CREATED

        self.render_response(result, req, resp, status_code)


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

    def get_object(self, req, resp, path_params, for_update=False, db_session=None):
        """
        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response

        :param path_params: path params extracted from URL path
        :type path_params: dict

        :param for_update: if the object is going to be updated or deleted
        :type for_update: bool

        :param db_session: SQLAlchemy session
        :type db_session: sqlalchemy.orm.session.Session
        """
        query = db_session.query(self.objects_class)
        if for_update:
            query = query.with_for_update()

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
        with self.session_scope(self.db_engine) as db_session:
            obj = self.get_object(req, resp, kwargs, db_session=db_session)

            result = self.serialize(obj,
                                    relations_include=relations,
                                    relations_ignore=list(getattr(self, 'serialize_ignore', [])))

        self.render_response(result, req, resp)

    def on_head(self, req, resp, *args, **kwargs):
        with self.session_scope(self.db_engine) as db_session:
            # call get_object to check if it exists
            self.get_object(req, resp, kwargs, db_session=db_session)

        resp.status = falcon.HTTP_NO_CONTENT

    def delete(self, req, resp, obj, db_session=None):
        """
        Delete an existing record.
        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response

        :param obj: the object to delete

        :param db_session: SQLAlchemy session
        :type db_session: sqlalchemy.orm.session.Session
        """
        deleted = db_session.delete(obj)
        if deleted == 0:
            raise falcon.HTTPConflict('Conflict', 'Resource found but conditions violated')

    def on_delete(self, req, resp, *args, **kwargs):
        try:
            with self.session_scope(self.db_engine) as db_session:
                obj = self.get_object(req, resp, kwargs, for_update=True, db_session=db_session)

                self.delete(req, resp, obj, db_session)
        except (IntegrityError, ProgrammingError) as err:
            # This should only be caused by foreign key constraint being violated
            if isinstance(err, IntegrityError) or err.orig.args[1] == self.VIOLATION_FOREIGN_KEY:
                raise HTTPConflict('Conflict', 'Other content links to this')
            else:
                raise

        self.render_response({}, req, resp)

    def update(self, req, resp, data, obj, db_session=None):
        """
        Create a new or update an existing record using provided data.
        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response

        :param data:
        :type data: dict

        :param obj: the object to update

        :param db_session: SQLAlchemy session
        :type db_session: sqlalchemy.orm.session.Session

        :return: created or updated object, serialized to a dict
        :rtype: dict
        """
        relations = self.clean_relations(self.get_param_or_post(req, self.PARAM_RELATIONS, ''))
        resource = self.save_resource(obj, data, db_session)
        db_session.commit()
        return self.serialize(resource, relations_include=relations,
                              relations_ignore=list(getattr(self, 'serialize_ignore', [])))

    def on_put(self, req, resp, *args, **kwargs):
        status_code = falcon.HTTP_OK
        try:
            with self.session_scope(self.db_engine) as db_session:
                obj = self.get_object(req, resp, kwargs, for_update=True, db_session=db_session)

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
