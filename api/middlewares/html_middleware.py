import falcon

from falcon.util.uri import parse_query_string
from jinja2 import TemplateNotFound


class HtmlTranslator(object):
    """
    Parses POST data in requests and renders HTML in responses using Jinja templates.

    When using plain HTML pages for resources it's not possible to use other HTTP verbs than GET and POST.
    Extra steps needs to be taken when preparing resources:

    * set RELATIONS_AS_LIST = True
    * add a templates property with a verb to file map, possibly using `objects_class` argument in init()
    * call `on_post()` in `on_put()`
    * perform a redirect after `on_post()`: :code:`resp.status = HTTP_FOUND; resp.set_header('Location', req.path)`
    """
    def __init__(self, template_env):
        self.template_env = template_env

    def process_resource(self, req, resp, resource, params):
        """
        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response

        :param resource:
        :type resource: api.resources.base.BaseCollectionResource|api.resources.base.BaseSingleResource

        :param params: parameters dict
        :type params: dict
        """
        if resource is None or req.method not in ['POST', 'PUT', 'PATCH']:
            return

        body = req.stream.read()
        if not body:
            raise falcon.HTTPBadRequest('Empty request body', 'At least one value is required')
        try:
            body = body.decode('utf-8')
        except UnicodeDecodeError:
            raise falcon.HTTPBadRequest('Invalid request body', 'A valid UTF-8 encoded document is required')

        req.context['doc'] = parse_query_string(body, keep_blank_qs_values=True, parse_qs_csv=False)

    def process_response(self, req, resp, resource):
        """
        :param req: Falcon request
        :type req: falcon.request.Request

        :param resp: Falcon response
        :type resp: falcon.response.Response

        :param resource:
        :type resource: reviews.resources.TemplateCollectionResource|reviews.resources.TemplateSingleResource
        """
        if resp.body or resp.status in [falcon.HTTP_MOVED_PERMANENTLY, falcon.HTTP_FOUND]:
            return

        if resource is None:
            template = self.get_error_template(resp.status)
        else:
            template = self.template_env.get_template(resource.templates[req.method])
        resp.body = template.render(**self.get_template_vars({'result': resp.body}, req))

    def get_error_template(self, status):
        filename = status.lower().replace(' ', '_') + '.html'
        try:
            return self.template_env.get_template(filename)
        except TemplateNotFound:
            return self.template_env.get_template('error.html')

    @staticmethod
    def get_template_vars(base_vars, req):
        """
        :param base_vars:
        :type base_vars: dict

        :param req: Falcon request
        :type req: falcon.request.Request

        :return:
        :rtype: dict
        """
        template_vars = base_vars.copy()
        template_vars['path'] = req.path
        template_vars['params'] = req.params
        return template_vars
