from urllib.parse import urljoin

import falcon


class IndexResource(object):
    """
    Lists routes registered in current app.
    """
    def __init__(self, routes):
        """
        :param routes: list of routes registered in current API
        :type routes: list
        """
        self.routes = routes

    def on_get(self, req, resp):
        """
        Lists routes registered in current app.

        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response
        """
        resp.body = [urljoin(req.uri, route) for route in self.routes]
        resp.status = falcon.HTTP_200
