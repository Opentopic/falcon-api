try:
    import ujson as json
except ImportError:
    import json
import falcon
import logging


class RequireJSON(object):
    """
    Checks if the request has required content type and client accepts a JSON response.
    """

    def process_request(self, req, resp):
        """
        :param req: Falcon request
        :type req: falcon.request.Request
        :param resp: Falcon response
        :type resp: falcon.response.Response
        """
        if not req.client_accepts_json:
            raise falcon.HTTPNotAcceptable(
                'This API only supports responses encoded as JSON.',
                href='http://docs.examples.com/api/json')

        if req.method in ('POST', 'PUT', 'PATCH'):
            if req.content_type is None or 'application/json' not in req.content_type:
                raise falcon.HTTPUnsupportedMediaType(
                    'This API only supports requests encoded as JSON.',
                    href='http://docs.examples.com/api/json')


class JSONTranslator(object):
    """
    Converts request input data and response to/from JSON.
    """
    def process_request(self, req, resp):
        """
        Converts request input data from JSON to a dict.
        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response
        """
        # req.stream corresponds to the WSGI wsgi.input environ variable,
        # and allows you to read bytes from the request body.
        #
        # See also: PEP 3333
        if req.content_length in (None, 0):
            # Nothing to do
            return

        body = req.stream.read()
        if not body:
            raise falcon.HTTPBadRequest('Empty request body',
                                        'A valid JSON document is required.')

        try:
            req.context['doc'] = json.loads(body.decode('utf-8'))

        except (ValueError, UnicodeDecodeError):
            raise falcon.HTTPError(falcon.HTTP_753,
                                   'Malformed JSON',
                                   'Could not decode the request body. The '
                                   'JSON was incorrect or not encoded as '
                                   'UTF-8.')

    def process_response(self, req, resp, resource, req_succeeded):
        """
        Converts response body to JSON.
        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response

        :param resource:
        :type resource: falcon_dbapi.resources.base.BaseResource

        :param req_succeeded:
        :type req_succeeded: bool
        """
        if req_succeeded:
            resp.body = json.dumps(resp.body)


class JsonError(Exception):
    """
    Converts all exceptions to falcon.HTTPError so they're serializable and match requested content type.

    Usage:

    .. code-block:: python

        app.add_error_handler(Exception, JsonError.handle)
    """
    @staticmethod
    def handle(ex, req, resp, params):
        """
        :param ex: the exception
        :type ex: Exception

        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response

        :param params: parameters dict
        :type params: dict

        :raises falcon.HTTPError
        """
        logger = logging.getLogger()
        if not isinstance(ex, falcon.HTTPError):
            logger.error('Non-HTTP exception occurred ({}): {}'.format(type(ex), ex), exc_info=ex)
            ex = falcon.HTTPError(falcon.HTTP_500, str(type(ex)), str(ex))
        else:
            logger.warning('HTTP exception occurred ({}): {}'.format(type(ex), ex), exc_info=ex)
        raise ex
