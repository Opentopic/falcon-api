import json
from falcon import HTTPBadRequest, HTTPNotFound
from api.resources.base import BaseCollectionResource, BaseSingleResource


class CollectionResource(BaseCollectionResource):
    def get_queryset(self, req, resp):
        order = self.get_param_or_post(req, self.PARAM_ORDER)
        if '__raw__' in req.params:
            try:
                req.params['__raw__'] = json.loads(req.params['__raw__'])
            except ValueError:
                raise HTTPBadRequest('Invalid attribute', 'Value of {} filter attribute is invalid'.format('__raw__'))

        queryset = self.objects_class.objects(**req.params)
        if order:
            queryset.order_by(order)
        return queryset

    def create(self, req, resp, data):
        obj = self.objects_class(**data)
        obj.save()
        return self.serialize(obj)


class SingleResource(BaseSingleResource):
    def get_object(self, req, resp, path_params):
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
