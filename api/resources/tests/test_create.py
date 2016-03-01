import unittest

from falcon import Request, Response
from falcon.testing import create_environ

from api.exceptions import ParamException
from api.resources.create import CreateResource


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

    def test_get_data_without_expected_params(self):
        """
        test if :func:`CreateResource.get_data` will return correct data if there is no expected params,
        means no data no errors
        """
        resource = CreateResource(objects_class=None, expected_params=[])
        env = create_environ(path='/')
        req = Request(env)
        req.context = {
            'doc': {'id': 1, 'name': 'Opentopic'}
        }
        resp = Response()

        data, errors = resource.get_data(req, resp)
        self.assertEqual(data, {})
        self.assertEqual(errors, {})

    def test_get_data_with_expected_params_but_no_data(self):
        """
        test if :func:`CreateResource.get_data` will act correctly if expected params are not going to be send
        in request. By default expected params != required params so it just should return dict with empty values
        """

        resource = CreateResource(objects_class=None, expected_params=['id', 'name'])
        env = create_environ(path='/')
        req = Request(env)
        req.context = {
            'doc': {}
        }
        resp = Response()

        data, errors = resource.get_data(req, resp)
        self.assertEqual(data, {'id': None, 'name': None})
        self.assertEqual(errors, {})

    def test_get_data_with_send_and_expected_params(self):
        """
        test if :func:`CreateResource.get_data` will act correctly if expected params are in returned data
         when they are send as well
        """
        resource = CreateResource(objects_class=None, expected_params=['id', 'name'])
        env = create_environ(path='/')
        req = Request(env)
        req.context = {
            'doc': {
                'id': 1,
                'name': 'Opentopic'
            }
        }
        resp = Response()

        data, errors = resource.get_data(req, resp)
        self.assertEqual(data, {
            'id': 1,
            'name': 'Opentopic'
        })
        self.assertEqual(errors, {})

    def test_get_data_check_error_raising(self):
        """
        check if :func:`CreateResource.get_data` return errors dict when `clean_param_name` raise `ParamException`
        """
        resource = CreateResource(objects_class=None, expected_params=['id', 'name'])
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

        data, errors = resource.get_data(req, resp)
        self.assertEqual(data, {
            'id': 1,
        })
        self.assertEqual(errors, {'name': 'Test Error Message'})

    def test_on_put_success_result(self):
        """
        test :func:`CreateResource.on_put` if we will receive correct response
        """

        resource = CreateResource(objects_class=FakeObjectClass, expected_params=['id', 'name'])
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

        resource = CreateResource(objects_class=FakeObjectClass, expected_params=['id', 'name'])
        env = create_environ(path='/')
        req = Request(env)
        req.context = {
            'doc': {
                'id': 1,
            }
        }

        def clean_name_test(self):
            raise ParamException('Test Error Message')

        resource.clean_name = clean_name_test

        resp = Response()
        resource.on_put(req=req, resp=resp)
        self.assertEqual(
            req.context['result'],
            {'errors': {'name': 'Test Error Message'}}
        )
