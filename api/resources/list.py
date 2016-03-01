from api.resources.base import BaseResource


class ListResource(BaseResource):
    """
    Resource that makes it's easy to return list of objects.
    """

    def __init__(self, objects_class, max_limit=200):
        """
        :param objects_class: class represent single element of object lists that suppose to be returned
        :param max_limit: max limit of elements that suppose to be returned by default
        """
        self.objects_class = objects_class
        self.max_limit = max_limit

    def get_queryset(self, req, resp):
        """
        :param req: request object from falcon
        :param resp: response object from falcon

        :return: queryset from `object_class` for different engines might look different
        """
        return self.objects_class.objects

    @staticmethod
    def get_total_objects(queryset):
        """
        for each db engine this can be different but easy to extend by default
        support mongoengine

        :param queryset: queryset object from :func:`get_queryset`
        :return: int total number of objects returned by this queryset
        """
        return queryset.count()

    def get_object_list(self, queryset, limit=None, offset=None):
        """
        This might be very specyfic for mongoengine if you want use other engine could happened that you will have
        to extend it

        :param queryset: queryset from :func:`get_queryset`
        :param limit: int number of elements to return
        :param offset: int slice list of element at the beginning
        :return: sliced results based on `limit` and `offset` if it's None then we use `max_limit`
        """
        if limit or offset:
            if limit and offset:
                object_list = queryset[int(offset):int(limit) + int(offset)]
            elif limit:
                object_list = queryset[:int(limit)]
            else:
                object_list = queryset[int(offset):self.max_limit + int(offset)]
        else:
            object_list = queryset[:self.max_limit]
        return object_list

    def on_get(self, req, resp):
        """
        view responsible for returning list of element

        :param req: request from falcon
        :param resp: response from falcon
        """
        queryset = self.get_queryset(req, resp)
        total = self.get_total_objects(queryset=queryset)

        # limit results
        limit = req.context['doc'].get('limit')
        offset = req.context['doc'].get('offset')

        object_list = self.get_object_list(queryset=queryset, limit=limit, offset=offset)

        result = {
            'results': [self.to_json(obj) for obj in object_list],
            'total': total,
            'returned': len(object_list)
        }
        self.render_response(
            result=result,
            req=req,
            resp=resp
        )
