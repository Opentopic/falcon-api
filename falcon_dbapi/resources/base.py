try:
    import ujson as json
except ImportError:
    import json

import falcon

from falcon_dbapi.exceptions import ParamException


class BaseResource(object):
    """
    Base resource class that you would probably want to use to extend all of your other resources
    """

    def __init__(self, objects_class):
        """
        :param objects_class: class represent single element of object lists that suppose to be returned
        """
        self.objects_class = objects_class

    @staticmethod
    def render_response(result, req, resp, status=falcon.HTTP_OK):
        """
        :param result: Data to be returned in the response

        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response

        :param status: HTTP status code
        :type status: str
        """
        resp.body = result
        resp.status = status

    def serialize(self, obj):
        """
        Converts the object to an external representation.
        If the object is serializable, no conversion is necessary.

        :param obj: single instance of `objects_class`

        :return: python json serializable object like dicts / lists / strings / ints and so on...

        Example:

        .. code-block:: python

            return {'id': obj.id, 'name': obj.name}

        """
        return obj

    def deserialize(self, data):
        """
        Converts an external representation to values that can be assigned to an instance of `objects_class`.

        :param data: a dictionary
        :type data: dict

        :return: a dictionary with converted values
        :rtype: dict
        """
        if data is None:
            return {}

        return data

    def get_schema(self, objects_class):
        """
        Gets a JSON Schema (http://json-schema.org) for current objects class.

        :param objects_class: class represent single element of object lists that suppose to be returned

        :return: a JSON Schema
        :rtype: dict
        """
        raise NotImplementedError

    def clean(self, data):
        """
        Called after :func:`deserialize`, might perform more complex data filtering and validation.

        :param data:
        :type data: dict

        :return: a tuple of data and errors after additional cleanup
        """
        errors = {}
        result = {}
        for key, value in data.items():
            valid_func = getattr(self, 'clean_%s' % key, None)
            if not valid_func:
                result[key] = value
                continue
            try:
                result[key] = valid_func(value)
            except ParamException as e:
                errors.setdefault(key, []).append(str(e))
        return result, errors

    def get_param_or_post(self, req, name, default=None, pop_params=True):
        """
        Gets specified param from request params or body.
        If found in params, it's removed.

        :param req: Falcon request
        :type req: falcon.request.Request

        :param name: param name
        :type name: str

        :param default: Default value

        :param pop_params: if True, will pop from req params
        :type pop_params: bool

        :return: param extracted from query params or request body
        """
        if name in req.params:
            return req.params.pop(name) if pop_params else req.params.get(name)
        elif 'doc' in req.context:
            return req.context['doc'].get(name, default)
        return default

    def on_options(self, req, resp, **kwargs):
        """
        Returns allowed methods in the Allow HTTP header.
        Also returns a JSON Schema, if supported by current resource.

        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response
        """
        allowed_methods = []

        for method in falcon.HTTP_METHODS:
            try:
                responder = getattr(self, 'on_' + method.lower())
            except AttributeError:
                # resource does not implement this method
                pass
            else:
                # Usually expect a method, but any callable will do
                if callable(responder):
                    allowed_methods.append(method)

        resp.set_header('Allow', ', '.join(sorted(allowed_methods)))

        result = {'name': self.objects_class.__name__}
        if self.objects_class.__doc__:
            result['description'] = self.objects_class.__doc__.strip()
        try:
            result['schema'] = self.get_schema(self.objects_class)
        except NotImplementedError:
            pass
        self.render_response(result, req, resp)


class BaseCollectionResource(BaseResource):
    """
    Base resource class for working with collections of records.
    Allows to:
    * GET - fetch a list of records, filtered by using query params
    * POST - create a new record
    """
    PARAM_LIMIT = 'limit'
    PARAM_OFFSET = 'offset'
    PARAM_ORDER = 'order'
    PARAM_TOTAL_COUNT = 'total_count'
    PARAM_TOTALS = 'totals'
    PARAM_SEARCH = 'search'
    PARAM_TEXT_QUERY = 'q'
    AGGR_GROUPBY = 'group_by'
    AGGR_GROUPLIMIT = 'group_limit'

    def __init__(self, objects_class, max_limit=None):
        """
        :param objects_class: class represent single element of object lists that suppose to be returned

        :param max_limit: max limit of elements that suppose to be returned by default
        :type max_limit: int
        """
        super().__init__(objects_class)
        self.max_limit = max_limit

    def get_param_totals(self, req):
        """
        Gets the totals and total_count params and normalizes them into a single list.

        :param req: Falcon request
        :type req: falcon.request.Request

        :return: total expressions
        :rtype: list
        """
        totals = self.get_param_or_post(req, self.PARAM_TOTALS, [])
        if totals:
            if isinstance(totals, str):
                totals = json.loads(totals)
            if isinstance(totals, dict):
                totals = [totals]
            else:
                totals = list(map(lambda x: x if isinstance(x, dict) else {x: None}, totals))
        total_count = self.get_param_or_post(req, self.PARAM_TOTAL_COUNT)
        if total_count and not list(filter(lambda x: 'count' in x, totals)):
            totals.append({'count': None})
        return totals

    def get_queryset(self, req, resp):
        """
        Return a query object used to fetch data.

        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response

        :return: a query from `object_class`
        """
        raise NotImplementedError

    def get_total_objects(self, queryset, totals):
        """
        Return total number of results in a query.

        :param queryset: queryset object from :func:`get_queryset`

        :param totals: a list of dicts with aggregate function as key and column as value
        :type totals: list

        :return: dict with totals calculated in this query, ex. total_count with number of results
        :rtype: dict
        """
        if not totals:
            return {}
        for total in totals:
            if len(total) > 1 or 'count' not in total or total['count'] is not None:
                raise falcon.HTTPBadRequest('Invalid attribute', 'Only _count_ is supported in the _totals_ param')
        return {'total_count': queryset.count()}

    def get_object_list(self, queryset, limit=None, offset=None):
        """
        Return a list of objects returned from a query.

        :param queryset: queryset from :func:`get_queryset`

        :param limit: number of elements to return, `max_limit` will be used if None
        :type limit: int

        :param offset: slice list of element at the beginning
        :type offset: int

        :return: sliced results based on `limit` and `offset`
        """
        if limit is None:
            limit = self.max_limit
        offset = 0 if offset is None else max(offset, 0)
        if limit is not None:
            if self.max_limit is not None:
                limit = min(limit, self.max_limit)
            limit = max(limit, 0)
            return queryset[offset:limit + offset]
        return queryset[offset:]

    def get_data(self, req, resp):
        """
        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response
        """
        limit = self.get_param_or_post(req, self.PARAM_LIMIT, self.max_limit)
        offset = self.get_param_or_post(req, self.PARAM_OFFSET, 0)
        totals = self.get_param_totals(req)

        queryset = self.get_queryset(req, resp)
        totals = self.get_total_objects(queryset, totals)

        object_list = self.get_object_list(queryset, int(limit) if limit is not None else None, int(offset))

        return object_list, totals

    def on_get(self, req, resp):
        """
        Gets a list of records.

        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response
        """
        object_list, totals = self.get_data(req, resp)

        result = {'results': [self.serialize(obj) for obj in object_list],
                  'total': totals.pop('total_count') if 'total_count' in totals else None,
                  'returned': len(object_list)}
        result.update(totals)
        headers = {'x-api-total': str(result['total']) if result['total'] is not None else '',
                   'x-api-returned': str(result['returned'])}
        resp.set_headers(headers)
        self.render_response(result, req, resp)

    def on_head(self, req, resp):
        """
        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response
        """
        object_list, totals = self.get_data(req, resp)

        headers = {'x-api-total': str(totals.pop('total_count')) if 'total_count' in totals else '',
                   'x-api-returned': str(len(object_list))}
        resp.set_headers(headers)
        resp.status = falcon.HTTP_NO_CONTENT

    def create(self, req, resp, data):
        """
        Create a new or update an existing record using provided data.
        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response

        :param data:
        :type data: dict

        :return: created or updated object
        """
        raise NotImplementedError

    def on_post(self, req, resp, *args, **kwargs):
        """
        Add (create) a new record to the collection.

        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response
        """
        data = self.deserialize(req.context['doc'] if 'doc' in req.context else None)
        data, errors = self.clean(data)
        if errors:
            result = {'errors': errors}
            status_code = falcon.HTTP_BAD_REQUEST
        else:
            result = self.create(req, resp, data)
            status_code = falcon.HTTP_CREATED

        self.render_response(result, req, resp, status_code)

    def on_put(self, req, resp, *args, **kwargs):
        """
        Add (create) a new record to the collection.

        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response
        """
        self.on_post(req, resp, *args, **kwargs)


class BaseSingleResource(BaseResource):
    """
    Base resource class for working with a single record.
    Allows to:
    * GET - fetch a single record, filtered by using query params
    * PUT - update a (whole) record
    * PATCH - update parts of a single record
    """

    def get_object(self, req, resp, path_params, for_update=False):
        """
        Return a single object.

        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response

        :param path_params: positional params from the api route
        :type path_params: dict

        :param for_update: if the object is going to be updated or deleted
        :type for_update: bool

        :return: a query from `object_class`
        """
        raise NotImplementedError

    def on_get(self, req, resp, *args, **kwargs):
        """
        Gets a single record.

        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response
        """
        obj = self.get_object(req, resp, kwargs)
        self.render_response(self.serialize(obj), req, resp)

    def on_head(self, req, resp, *args, **kwargs):
        """
        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response
        """
        # call get_object to check if it exists
        self.get_object(req, resp, kwargs)
        resp.status = falcon.HTTP_NO_CONTENT

    def delete(self, req, resp, obj):
        """
        Delete an existing record.
        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response

        :param obj: the object to delete
        """
        deleted = obj.delete()
        if deleted == 0:
            raise falcon.HTTPConflict('Conflict', 'Resource found but conditions violated')

    def on_delete(self, req, resp, *args, **kwargs):
        """
        Deletes a single record.

        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response
        """
        obj = self.get_object(req, resp, kwargs, for_update=True)

        self.delete(req, resp, obj)
        self.render_response({}, req, resp)

    def update(self, req, resp, data, obj):
        """
        Create a new or update an existing record using provided data.
        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response

        :param data:
        :type data: dict

        :param obj: the object to update

        :return: created or updated object
        """
        raise NotImplementedError

    def on_put(self, req, resp, *args, **kwargs):
        """
        Updates a single record.
        This should set all missing fields to default values, but we're not going to be so strict.

        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response
        """
        obj = self.get_object(req, resp, kwargs, for_update=True)

        data = self.deserialize(req.context['doc'] if 'doc' in req.context else None)
        data, errors = self.clean(data)
        if errors:
            result = {'errors': errors}
            status_code = falcon.HTTP_BAD_REQUEST
        else:
            result = self.update(req, resp, data, obj)
            status_code = falcon.HTTP_OK
        self.render_response(result, req, resp, status_code)

    def on_patch(self, req, resp, *args, **kwargs):
        """
        Updates a single record. Changes only specified fields.

        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response
        """
        return self.on_put(req, resp, *args, **kwargs)
