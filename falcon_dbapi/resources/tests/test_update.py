import unittest

from falcon import Request, Response
from falcon.testing import create_environ

from falcon_dbapi.resources.mongoengine import SingleResource


class FakeObject(object):
    """
    Fake object to mock instance of document
    """

    def __init__(self, pk):
        self.pk = pk
        self.name = 'OldName'
        self._saved = False

    def save(self):
        self._saved = True
        return self


class FakeObjectList(object):
    """
    Fake object list used to mock manager behaviour
    """

    def get(self, pk):
        return FakeObject(pk=pk)


def fake_serialize(obj):
    """fake object to json func"""
    return {
        'pk': obj.pk,
        'name': obj.name,
        '_saved': obj._saved
    }


class UpdateResourceTest(unittest.TestCase):
    """
    Testcase for :class:`falcon_dbapi.resources.create.CreateResource`
    """

    def test_update_withtout_pk(self):
        """
        test how update function will handle when we are not going to pass pk value
        """
        resource = SingleResource(objects_class=None)
        env = create_environ(path='/')
        req = Request(env)
        req.context = {
            'doc': {}
        }
        resp = Response()

        with self.assertRaises(Exception):
            resource.on_patch(req, resp)

    def test_update_get_object(self):
        """
        Test `get_object` func
        """
        resource = SingleResource(objects_class=None)
        env = create_environ(path='/')
        req = Request(env)
        req.context = {
            'doc': {'pk': 1}
        }
        resp = Response()

        resource.objects_class = FakeObjectList()
        obj = resource.get_object(req=req, resp=resp, path_params={})
        self.assertEqual(obj.pk, 1)

    def test_update_when_no_expected_params_is_set(self):
        """
        Test if update func will not update param if it's not defined in expected params
        """
        resource = SingleResource(objects_class=None)
        env = create_environ(path='/')
        req = Request(env)
        req.context = {
            'doc': {'pk': 1, 'name': 'TestNewName'}
        }

        def clean(data):
            return {}, {}

        resource.clean = clean

        resp = Response()

        resource.objects_class = FakeObjectList()
        resource.serialize = fake_serialize
        resource.on_patch(req, resp)
        self.assertEqual(
            {'pk': 1, 'name': 'OldName', '_saved': True},
            resp.body
        )

    def test_update_params(self):
        """
        Test if param is updated and returned
        """
        resource = SingleResource(objects_class=None)
        env = create_environ(path='/')
        req = Request(env)
        req.context = {
            'doc': {'pk': 1, 'name': 'TestNewName'},
        }
        resp = Response()

        resource.objects_class = FakeObjectList()
        resource.serialize = fake_serialize
        resource.on_patch(req, resp)
        self.assertEqual(
            {'pk': 1, 'name': 'TestNewName', '_saved': True},
            resp.body
        )
