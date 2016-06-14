from falcon import HTTPNotFound

from api.resources.base import BaseCollectionResource, BaseSingleResource


class CollectionResource(BaseCollectionResource):
    def get_queryset(self, req, resp):
        return self.objects_class.objects(**req.params)

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
