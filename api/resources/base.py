import falcon


class BaseResource(object):
    """
    Base resource class that you would probably want to use to extend all of your other resources
    """

    @staticmethod
    def render_response(result, req, resp):

        req.context['result'] = result
        resp.status = falcon.HTTP_200

    def to_json(self, obj):
        """
        We assume that by default obj is serializable
        :param obj: single instnace of `objectc_class`
        :return: python json serializable object like dicts / lists / strings / ints and so on...

        Example:

        .. code-block:: python

            return {'id': obj.id, 'name': obj.name}

        """
        return obj