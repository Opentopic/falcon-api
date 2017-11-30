import unittest

from falcon import Request, Response
from falcon.testing import create_environ

from falcon_dbapi.exceptions import ParamException
from falcon_dbapi.resources.mongoengine import CollectionResource


class FakeObjectClass(dict):
    """
    Fake object class just to test if object is properly saved
    """

    def save(self):
        return self


class CreateResourceTest(unittest.TestCase):
    def test_clean_check_error_raising(self):
        """
        Check if clean function returns errors dict when `clean_param_name` raise `ParamException`
        """
        resource = CollectionResource(objects_class=None)
        env = create_environ(path='/')
        req = Request(env)
        req.context = {
            'doc': {
                'id': 1,
                'name': 'Opentopic'
            }
        }
        Response()

        def clean_name_test(self):
            raise ParamException('Test Error Message')

        resource.clean_name = clean_name_test

        data, errors = resource.clean(req.context['doc'])
        self.assertEqual(data, {
            'id': 1,
        })
        self.assertEqual(errors, {'name': ['Test Error Message']})

    def test_on_put_success_result(self):
        """
        Test if we will receive correct response
        """
        resource = CollectionResource(objects_class=FakeObjectClass)
        env = create_environ(path='/')
        req = Request(env)
        req.context = {
            'doc': {
                'id': 1,
                'name': 'Opentopic'
            }
        }
        resp = Response()
        resource.on_put(req=req, resp=resp)
        self.assertEqual(
            resp.body,
            {'id': 1, 'name': 'Opentopic'}
        )

    def test_on_put_error_result(self):
        """
        Test if we will receive correct response
        """

        resource = CollectionResource(objects_class=FakeObjectClass)
        env = create_environ(path='/')
        req = Request(env)
        req.context = {
            'doc': {
                'id': 1,
            }
        }

        def clean(data):
            if 'name' not in data:
                return {}, {'name': ['Test Error Message']}

        resource.clean = clean

        resp = Response()
        resource.on_put(req=req, resp=resp)
        self.assertEqual(
            resp.body,
            {'errors': {'name': ['Test Error Message']}}
        )
