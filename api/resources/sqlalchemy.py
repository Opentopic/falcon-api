from contextlib import contextmanager
from datetime import datetime, time
from decimal import Decimal
from falcon import HTTPConflict, HTTPBadRequest, HTTPNotFound
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.base import MANYTOONE
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.sql import sqltypes, operators, extract

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
        'day': lambda c, x: extract('day', c) == x
    }

    def serialize(self, obj, skip_primary_key=False, skip_foreign_keys=False, relations_level=1, relations_ignore=None):
        """
        Converts the object to a serializable dictionary.
        :param obj: the object to serialize

        :param skip_primary_key: should primary keys be skipped
        :type skip_primary_key: bool

        :param skip_foreign_keys: should foreign keys be skipped
        :type skip_foreign_keys: bool

        :param relations_level: how many levels of relations to serialize
        :type relations_level: int

        :param relations_ignore: relationship model classes to ignore
        :type relations_ignore: list

        :return: a serializable dictionary
        :rtype: dict
        """
        data = {}
        data = self.serialize_columns(obj, data, skip_primary_key, skip_foreign_keys)
        if relations_level > 0:
            if relations_ignore is None:
                relations_ignore = list(getattr(self, 'serialize_ignore', []))
            data = self.serialize_relations(obj, data, relations_level, relations_ignore)
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

    def serialize_relations(self, obj, data, relations_level=1, relations_ignore=None):
        mapper = inspect(obj).mapper
        if relations_ignore is None:
            relations_ignore = []
        relations_ignore.append(mapper.class_)
        for relation in mapper.relationships:
            if relation.mapper.class_ in relations_ignore:
                continue
            rel_obj = getattr(obj, relation.key)
            if rel_obj is None:
                continue
            if relation.direction == MANYTOONE:
                data[relation.key] = self.serialize(rel_obj,
                                                    relations_level=relations_level - 1,
                                                    relations_ignore=relations_ignore)
            elif not relation.uselist:
                data.update(self.serialize(rel_obj, skip_primary_key=True,
                                           relations_level=relations_level - 1,
                                           relations_ignore=relations_ignore))
            else:
                data[relation.key] = {
                    rel.id: self.serialize(rel, skip_primary_key=True,
                                           relations_level=relations_level - 1,
                                           relations_ignore=relations_ignore)
                    for rel in rel_obj
                }
        return data

    def deserialize(self, data):
        attributes = {}

        if data is None:
            return attributes

        mapper = inspect(self.objects_class)
        for key, value in data.items():
            if key in mapper.relationships:
                if isinstance(value, str):
                    attributes[key] = []
                    for v in value.split(','):
                        try:
                            attributes[key].push(int(v))
                        except ValueError:
                            pass
                else:
                    attributes[key] = value
            elif key in mapper.columns:
                attributes[key] = self.deserialize_column(mapper.columns[key], value)

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

    def filter_by(self, query, **kwargs):
        """
        :param query: SQLAlchemy Query object
        :type query: sqlalchemy.orm.query.Query
        :return: modified query
        :rtype: sqlalchemy.orm.query.Query
        """
        return self._filter_or_exclude(query, False, **kwargs)

    def exclude_by(self, query, **kwargs):
        """
        :param query: SQLAlchemy Query object
        :type query: sqlalchemy.orm.query.Query
        :return: modified query
        :rtype: sqlalchemy.orm.query.Query
        """
        return self._filter_or_exclude(query, True, **kwargs)

    def _filter_or_exclude(self, query, negate, **kwargs):
        """
        :param query: SQLAlchemy Query object
        :type query: sqlalchemy.orm.query.Query
        :param negate: should the filter expressions be negated
        :type negate: bool
        :return: modified query
        :rtype: sqlalchemy.orm.query.Query
        """
        def negate_if(expr):
            return expr if not negate else ~expr
        column = None
        column_name = None
        obj_class = self.objects_class
        mapper = inspect(obj_class)

        for arg, value in kwargs.items():
            if arg == CollectionResource.PARAM_LIMIT or arg == CollectionResource.PARAM_OFFSET:
                continue
            for token in arg.split('__'):
                if column_name is not None and token in self._underscore_operators:
                    op = self._underscore_operators[token]
                    if op in [operators.between_op, operators.in_op]:
                        value = list(
                            map(lambda x: self.deserialize_column(column, x), value.split(self.MULTIVALUE_SEPARATOR)))
                    else:
                        value = self.deserialize_column(column, value)
                    query = query.filter(negate_if(op(column_name, value)))
                    # reset column, obj_class and mapper back to main object
                    column_name = None
                    obj_class = self.objects_class
                    mapper = inspect(obj_class)
                    continue
                if token in mapper.relationships:
                    # follow the relation and change current obj_class and mapper
                    obj_class = mapper.relationships[token].mapper.class_
                    mapper = mapper.relationships[token].mapper
                    query = query.join(token, aliased=True, from_joinpoint=True)
                    continue
                if token not in mapper.column_attrs:
                    # if token is not an op or relation it has to be a valid column
                    raise HTTPBadRequest('Invalid attribute', 'An attribute provided for filtering is invalid')
                column_name = getattr(obj_class, token, None)
                """:type column: sqlalchemy.schema.Column"""
                column = mapper.columns[token]
            if column_name is not None:
                # if last token was a column, not an op, assume it's equality
                # if it was a relation it's just going to be ignored
                value = self.deserialize_column(column, value)
                query = query.filter(negate_if(column_name == value))
            query = query.reset_joinpoint()
            # reset everything back to main object
            column_name = None
            obj_class = self.objects_class
            mapper = inspect(obj_class)
        return query


class CollectionResource(AlchemyMixin, BaseCollectionResource):
    VIOLATION_UNIQUE = '23505'

    def __init__(self, objects_class, db_engine, max_limit=None):
        """
        :param objects_class: class represent single element of object lists that suppose to be returned
        :param db_engine: SQL Alchemy engine
        :type db_engine: sqlalchemy.engine.Engine
        """
        super(CollectionResource, self).__init__(objects_class, max_limit)
        self.db_engine = db_engine

    def get_queryset(self, req, resp, db_session=None):
        return self.filter_by(db_session.query(self.objects_class), **req.params)

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

        with session_scope(self.db_engine) as db_session:
            query = self.get_queryset(req, resp, db_session)
            total = self.get_total_objects(query)

            object_list = self.get_object_list(query, limit, offset)

            result = {
                'results': [self.serialize(obj) for obj in object_list],
                'total': total,
                'returned': object_list.count(),
            }

        self.render_response(result, req, resp)

    def create(self, req, resp, data):
        try:
            with session_scope(self.db_engine) as db_session:
                # replace any relations with objects instead of pks
                mapper = inspect(self.objects_class)
                for key, value in data.items():
                    if key not in mapper.relationships:
                        continue
                    related_mapper = mapper.relationships[key].mapper
                    data[key] = db_session.query(related_mapper.class_).filter(
                        related_mapper.primary_key[0].in_(value)).all()

                # create and save the object
                resource = self.objects_class(**data)
                db_session.add(resource)
                db_session.commit()
                return self.serialize(resource)
        except (IntegrityError, ProgrammingError) as err:
            # Cases such as unallowed NULL value should have been checked before we got here (e.g. validate against
            # schema using falconjsonio) - therefore assume this is a UNIQUE constraint violation
            if isinstance(err, IntegrityError) or err.orig.args[1] == self.VIOLATION_UNIQUE:
                raise HTTPConflict('Conflict', 'Unique constraint violated')
            else:
                raise


class SingleResource(AlchemyMixin, BaseSingleResource):
    VIOLATION_FOREIGN_KEY = '23503'

    def __init__(self, objects_class, db_engine):
        """
        :param objects_class: class represent single element of object lists that suppose to be returned
        :param db_engine: SQL Alchemy engine
        :type db_engine: sqlalchemy.engine.Engine
        """
        super(SingleResource, self).__init__(objects_class)
        self.db_engine = db_engine

    def get_object(self, req, resp, path_params, db_session=None):
        query = db_session.query(self.objects_class)

        for key, value in path_params.items():
            attr = getattr(self.objects_class, key, None)
            query = query.filter(attr == value)

        query = self.filter_by(query, **req.params)

        try:
            obj = query.one()
        except NoResultFound:
            raise HTTPNotFound()
        except MultipleResultsFound:
            raise HTTPBadRequest('Multiple results', 'Query params match multiple records')
        return obj

    def on_get(self, req, resp, *args, **kwargs):
        with session_scope(self.db_engine) as db_session:
            obj = self.get_object(req, resp, kwargs, db_session)

            result = {
                'results': self.serialize(obj),
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
        for key, value in data.items():
            setattr(obj, key, value)
        db_session.add(obj)
        db_session.commit()
        return self.serialize(obj)

    def on_put(self, req, resp, *args, **kwargs):
        try:
            with session_scope(self.db_engine) as db_session:
                obj = self.get_object(req, resp, kwargs, db_session)

                data = self.deserialize(req.context['doc'] if 'doc' in req.context else None)
                data, errors = self.clean(data)
                if errors:
                    result = {'errors': errors}
                else:
                    result = self.update(req, resp, data, obj, db_session)
        except (IntegrityError, ProgrammingError) as err:
            # Cases such as unallowed NULL value should have been checked before we got here (e.g. validate against
            # schema using falconjsonio) - therefore assume this is a UNIQUE constraint violation
            if isinstance(err, IntegrityError) or err.orig.args[1] == self.VIOLATION_FOREIGN_KEY:
                raise HTTPConflict('Conflict', 'Unique constraint violated')
            else:
                raise

        self.render_response(result, req, resp)
