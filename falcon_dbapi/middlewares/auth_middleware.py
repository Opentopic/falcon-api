import falcon


class AuthMiddleware(object):
    """
    Token based authentication that uses two HTTP headers: X-Project-ID and X-Auth-Token.
    """
    def __init__(self, prefix, tokens, noauth_routes=None):
        self.prefix = prefix
        self.tokens = tokens
        self.noauth_routes = noauth_routes or []

    def process_request(self, req, resp):
        """
        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response

        :raises falcon.HTTPUnauthorized
        """
        if (self.prefix and not req.path.startswith(self.prefix)) or req.path in self.noauth_routes:
            return

        token = req.get_header('X-Auth-Token')
        project = req.get_header('X-Project-ID')

        if token is None:
            description = 'Please provide an auth token as part of the request.'

            raise falcon.HTTPUnauthorized('Auth token required', description, None)

        if not self._token_is_valid(token, project):
            description = 'The provided auth token is not valid. Please request a new token and try again.'

            raise falcon.HTTPUnauthorized('Authentication required', description, None)

    def _token_is_valid(self, token, project):
        """
        :param token:
        :type token: string

        :param project:
        :type project: string

        :return: true if project and token matches against self.tokens
        :rtype: bool
        """
        return project in self.tokens and token in self.tokens[project]
