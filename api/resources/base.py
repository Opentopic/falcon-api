import falcon

from api.exceptions import ParamException


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
        req.context['result'] = result
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

    def clean(self, data):
        """
        Called after :func:`deserialize`, might perform more complex data filtering and validation.

        :param data:
        :type data: dict

        :return: a tuple of data and errors after additional cleanup
        """
        errors = {}
        for key, value in data.items():
            valid_func = getattr(self, 'clean_%s' % key, None)
            if not valid_func:
                continue
            try:
                data[key] = valid_func(value)
            except ParamException as e:
                errors.setdefault(key, []).append(str(e))
        return data, errors


class BaseCollectionResource(BaseResource):
    """
    Base resource class for working with collections of records.
    Allows to:
    * GET - fetch a list of records, filtered by using query params
    * POST - create a new record
    """

    def __init__(self, objects_class, max_limit=None):
        """
        :param objects_class: class represent single element of object lists that suppose to be returned

        :param max_limit: max limit of elements that suppose to be returned by default
        """
        super().__init__(objects_class)
        self.max_limit = max_limit

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

    def get_total_objects(self, queryset):
        """
        Return total number of results in a query.

        :param queryset: queryset object from :func:`get_queryset`

        :return: total number of results returned by this query
        :rtype: int
        """
        return queryset.count()

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
        if offset is None:
            offset = 0
        limit = max(min(limit, self.max_limit), 0)
        offset = max(offset, 0)
        return queryset[offset:limit + offset]

    def on_get(self, req, resp):
        """
        Gets a list of records.

        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response
        """
        queryset = self.get_queryset(req, resp)
        total = self.get_total_objects(queryset)

        limit = int(req.context['doc'].get('limit', self.max_limit))
        offset = int(req.context['doc'].get('offset', 0))

        object_list = self.get_object_list(queryset, limit, offset)

        result = {
            'results': [self.serialize(obj) for obj in object_list],
            'total': total,
            'returned': len(object_list)
        }
        self.render_response(result, req, resp)

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
        else:
            result = self.create(req, resp, data)
        self.render_response(result, req, resp, falcon.HTTP_CREATED)

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

    def get_object(self, req, resp, path_params):
        """
        Return a single object.

        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response

        :param path_params: positional params from the api route
        :type path_params: dict

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

        result = {
            'results': self.serialize(obj),
        }
        self.render_response(result, req, resp)

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
        obj = self.get_object(req, resp, kwargs)

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
        obj = self.get_object(req, resp, kwargs)

        data = self.deserialize(req.context['doc'] if 'doc' in req.context else None)
        data, errors = self.clean(data)
        if errors:
            result = {'errors': errors}
        else:
            result = self.update(req, resp, obj, data)
        self.render_response(result, req, resp)

    def on_patch(self, req, resp, *args, **kwargs):
        """
        Updates a single record. Changes only specified fields.

        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response
        """
        return self.on_put(req, resp, *args, **kwargs)
