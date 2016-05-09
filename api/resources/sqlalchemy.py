from contextlib import contextmanager
from datetime import datetime, time
from decimal import Decimal
from falcon import HTTPConflict, HTTPBadRequest, HTTPNotFound
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound, MultipleResultsFound
from sqlalchemy.sql import sqltypes

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
    def serialize(self, obj, skip_primary_key=False, skip_foreign_keys=False):
        data = {}
        columns = inspect(obj).mapper.columns
        for key, column in columns.items():
            if skip_primary_key and column.primary_key:
                continue
            if skip_foreign_keys and len(column.foreign_keys):
                continue
            value = getattr(obj, key)
            if isinstance(value, datetime):
                value = value.strftime('%Y-%m-%dT%H:%M:%SZ')
            elif isinstance(value, time):
                value = value.isoformat()
            elif isinstance(value, Decimal):
                value = float(value)
            data[key] = value
        return data

    def deserialize(self, data):
        mapper = inspect(self.objects_class)
        attributes = {}

        if data is None:
            return attributes

        for key, value in data.items():
            column = mapper.columns[key]
            if isinstance(column.type, sqltypes.DateTime):
                attributes[key] = datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ') if value is not None else None
            elif isinstance(column.type, sqltypes.Time):
                if value is not None:
                    hour, minute, second = value.split(':')
                    attributes[key] = time(int(hour), int(minute), int(second))
                else:
                    attributes[key] = None
            else:
                attributes[key] = value

        return attributes

    def filter_by_params(self, resources, params):
        for filter_key, value in params.items():
            if filter_key == CollectionResource.PARAM_LIMIT or filter_key == CollectionResource.PARAM_OFFSET:
                continue
            filter_parts = filter_key.split('__')
            key = filter_parts[0]
            if len(filter_parts) == 1:
                comparison = '='
            elif len(filter_parts) == 2:
                comparison = filter_parts[1]
            else:
                raise HTTPBadRequest('Invalid attribute', 'An attribute provided for filtering is invalid')

            attr = getattr(self.objects_class, key, None)
            if attr is None or key not in inspect(self.objects_class).column_attrs:
                raise HTTPBadRequest('Invalid attribute', 'An attribute provided for filtering is invalid')
            if comparison == '=':
                resources = resources.filter(attr == value)
            elif comparison == 'null':
                if value != '0':
                    resources = resources.filter(attr.is_(None))
                else:
                    resources = resources.filter(attr.isnot(None))
            elif comparison == 'startswith':
                resources = resources.filter(attr.like('{0}%'.format(value)))
            elif comparison == 'contains':
                resources = resources.filter(attr.like('%{0}%'.format(value)))
            elif comparison == 'lt':
                resources = resources.filter(attr < value)
            elif comparison == 'lte':
                resources = resources.filter(attr <= value)
            elif comparison == 'gt':
                resources = resources.filter(attr > value)
            elif comparison == 'gte':
                resources = resources.filter(attr >= value)
            else:
                raise HTTPBadRequest('Invalid attribute', 'An attribute provided for filtering is invalid')
        return resources


class CollectionResource(AlchemyMixin, BaseCollectionResource):
    PARAM_LIMIT = 'limit'
    PARAM_OFFSET = 'offset'

    def __init__(self, objects_class, db_engine):
        """
        :param objects_class: class represent single element of object lists that suppose to be returned
        :param db_engine: SQL Alchemy engine
        :type db_engine: sqlalchemy.engine.Engine
        """
        super(CollectionResource, self).__init__(objects_class)
        self.db_engine = db_engine

    def get_queryset(self, req, resp, db_session=None):
        return self.filter_by_params(db_session.query(self.objects_class), req.params)

    def get_object_list(self, queryset, limit=None, offset=None):
        if limit is None:
            limit = self.max_limit
        if offset is None:
            offset = 0
        if limit is not None:
            limit = max(min(limit, self.max_limit), 0)
            queryset = queryset.limit(limit)
        offset = max(offset, 0)
        return queryset.offset(offset)

    def get_param_or_post(self, req, name, default=None):
        if name in req.params:
            return req.params[name]
        elif 'doc' in req.context:
            return req.context['doc'].get(name, default)
        return default

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
        resource = self.objects_class(**data)

        try:
            with session_scope(self.db_engine) as db_session:
                db_session.add(resource)
                db_session.commit()
                return self.serialize(resource)
        except (IntegrityError, ProgrammingError) as err:
            # Cases such as unallowed NULL value should have been checked before we got here (e.g. validate against
            # schema using falconjsonio) - therefore assume this is a UNIQUE constraint violation
            if isinstance(err, IntegrityError) or err.orig.args[1] == '23505':
                raise HTTPConflict('Conflict', 'Unique constraint violated')
            else:
                raise


class SingleResource(AlchemyMixin, BaseSingleResource):
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

        query = self.filter_by_params(query, req.params)

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
            if isinstance(err, IntegrityError) or err.orig.args[1] == '23503':
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
            if isinstance(err, IntegrityError) or err.orig.args[1] == '23505':
                raise HTTPConflict('Conflict', 'Unique constraint violated')
            else:
                raise

        self.render_response(result, req, resp)
