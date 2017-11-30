import unittest

from falcon import Request, Response
from falcon.testing import create_environ

from falcon_dbapi.resources.base import BaseResource


class BaseResourceTest(unittest.TestCase):
    """
    Testcase for :class:`falcon_dbapi.resources.base.BaseResource`
    """

    def test_render_response_status_200(self):
        """
        need to check if status of the response is set for 200 and
        """
        env = create_environ(path='/')
        req = Request(env)
        resp = Response()
        result = None
        BaseResource.render_response(
            result=result, req=req, resp=resp
        )
        self.assertTrue(resp.status, 200)

    def test_render_response_result(self):
        """
        check if result is available request context
        """
        env = create_environ(path='/')
        req = Request(env)
        resp = Response()
        result = 'Cognitive Digital Marketing :)'
        BaseResource.render_response(
            result=result, req=req, resp=resp
        )
        self.assertEqual(resp.body, 'Cognitive Digital Marketing :)')
