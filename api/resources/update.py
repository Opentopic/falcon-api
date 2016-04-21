from falcon import HTTPError

from api.resources.create import CreateResource


class UpdateResource(CreateResource):
    """
    Resource for object update
    """

    def get_object(self, req, resp):
        """
        get object to update
        :return: object to collect
        """
        pk = req.context['doc'].get('pk')
        if not pk:
            return None
        obj = self.objects_class.get(pk)
        return obj

    def on_put(self, req, resp):
        raise HTTPError(404)

    def on_patch(self, req, resp):
        obj = self.get_object(req, resp)

        if obj is None:
            errors = ['PK param was not provided']
        else:
            data, errors = self.get_data(req, resp)

        if errors:
            result = {'errors': errors}
        else:
            obj = self.save(obj=obj, **data)
            result = self.to_json(obj)
        self.render_response(
            result=result,
            req=req,
            resp=resp
        )

    def save(self, obj, **kwargs):
        """
        update object with values that was changed
        """
        for key, value in kwargs.items():
            setattr(obj, key, value)
        return obj.save()
