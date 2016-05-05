import unittest

from falcon import Request, Response
from falcon.testing import create_environ

from api.exceptions import ParamException
from api.resources.mongoengine import CollectionResource


class FakeObjectClass(dict):
    """
    Fake object class just to test if object is properly saved
    """

    def save(self):
        return self


class CreateResourceTest(unittest.TestCase):
    """
    Testcase for :class:`api.resources.create.CreateResource`
    """

    def test_clean_check_error_raising(self):
        """
        check if :func:`CreateResource.get_data` return errors dict when `clean_param_name` raise `ParamException`
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
        resp = Response()

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
        test :func:`CreateResource.on_put` if we will receive correct response
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
            req.context['result'],
            {'id': 1, 'name': 'Opentopic'}
        )

    def test_on_put_error_result(self):
        """
        test :func:`CreateResource.on_put` if we will receive correct response
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
            req.context['result'],
            {'errors': {'name': ['Test Error Message']}}
        )
