try:
    import ujson as json
except ImportError:
    import json
from falcon import HTTPBadRequest, HTTPNotFound
from falcon_dbapi.resources.base import BaseCollectionResource, BaseSingleResource


class CollectionResource(BaseCollectionResource):
    def get_queryset(self, req, resp):
        query_term = self.get_param_or_post(req, self.PARAM_TEXT_QUERY)
        search = self.get_param_or_post(req, self.PARAM_SEARCH)
        if search:
            try:
                req.params['__raw__'] = json.loads(search)
            except ValueError:
                raise HTTPBadRequest('Invalid attribute',
                                     'Value of {} filter attribute is invalid'.format(self.PARAM_SEARCH))
        order = self.get_param_or_post(req, self.PARAM_ORDER)
        queryset = self.objects_class.objects(**req.params)
        if query_term is not None:
            queryset = queryset.search_text(query_term)
        if order:
            queryset = queryset.order_by(order)
        return queryset

    def create(self, req, resp, data):
        obj = self.objects_class(**data)
        obj.save()
        return self.serialize(obj)


class SingleResource(BaseSingleResource):
    def get_object(self, req, resp, path_params, for_update=False):
        pk = req.context['doc'].get('pk')
        if not pk:
            raise HTTPNotFound()
        obj = self.objects_class.get(pk)
        if obj is None:
            raise HTTPNotFound()
        return obj

    def update(self, req, resp, data, obj):
        for key, value in data.items():
            setattr(obj, key, value)
        obj.save()
        return self.serialize(obj)
