import falcon
import logging

from api.middlewares.html_middleware import HtmlTranslator


class JsonError(Exception):
    """
    Converts all exceptions to falcon.HTTPError so they're serializable and match requested content type.

    Usage::
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


class HtmlError(Exception):
    """
    Renders a HTML template containing error details.

    Usage::
        handler = HtmlError(translator)
        app.add_error_handler(Exception, handler.handle)
    """
    def __init__(self, translator):
        """
        :param translator: a HTML translator instance
        :type translator: api.middlewares.html_middleware.HtmlTranslator
        """
        self.translator = translator

    def handle(self, ex, req, resp, params):
        """
        :param ex: the exception
        :type ex: Exception

        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response

        :param params: parameters dict
        :type params: dict
        """
        logger = logging.getLogger()
        if not isinstance(ex, falcon.HTTPError):
            logger.error('Non-HTTP exception occurred ({}): {}'.format(type(ex), ex), exc_info=ex)
            ex = falcon.HTTPError(falcon.HTTP_500, str(type(ex)), str(ex))
        template = self.translator.get_error_template(ex.status)
        resp.status = ex.status
        if ex.headers:
            resp.set_headers(ex.headers)
        resp.body = template.render(**HtmlTranslator.get_template_vars(ex.to_dict(), req))
