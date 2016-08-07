import falcon
import logging


class JsonError(Exception):
    """
    Converts all exceptions to falcon.HTTPError so they're serializable and match requested content type.
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
