import unittest
from unittest.mock import Mock

from falcon import Request, Response
from falcon.testing import create_environ
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

from api.resources import mongoengine


class FakeObjectList(list):
    """
    Extend list to match interface of List resource
    """

    @property
    def objects(self):
        return self

    def count(self):
        return len(self)


Base = declarative_base()


class DbModel(Base):
    __tablename__ = 'some_table'

    id = Column(Integer, primary_key=True)
    url = Column(String, nullable=False)


class MongoCollectionResourceTest(unittest.TestCase):
    """
    Testcase for :class:`api.resources.mongoengine.CollectionResource`
    """

    def test_get_object_list_no_limit_no_offset(self):
        """
        test collecting object_list when there is no limit or offset set
        """
        resource = mongoengine.CollectionResource(objects_class=None, max_limit=2)
        object_list = resource.get_object_list(queryset=[1, 2, 3, 4], limit=None, offset=None)
        self.assertEqual([1, 2], object_list)

    def test_get_object_list_with_limit(self):
        """
        test collecting object_list when there is limit set
        """
        resource = mongoengine.CollectionResource(objects_class=None, max_limit=2)
        object_list = resource.get_object_list(queryset=[1, 2, 3, 4], limit=3, offset=None)
        self.assertEqual([1, 2], object_list)

    def test_get_object_list_with_offset(self):
        """
        test collecting object_list when there is offset set
        """
        resource = mongoengine.CollectionResource(objects_class=None, max_limit=2)
        object_list = resource.get_object_list(queryset=[1, 2, 3, 4], limit=None, offset=1)
        self.assertEqual([2, 3], object_list)

    def test_get_object_list_limit_and_offset(self):
        """
        test collecting object_list when there is limit and offset set in a same time
        """
        resource = mongoengine.CollectionResource(objects_class=None, max_limit=2)
        object_list = resource.get_object_list(queryset=[1, 2, 3, 4], limit=3, offset=1)
        self.assertEqual([2, 3], object_list)

    def test_on_get(self):
        """
        need to check if status of the response is set for 200 and
        """
        env = create_environ(path='/')
        req = Request(env)
        req.context = {
            'doc': {}
        }
        resp = Response()
        resource = mongoengine.CollectionResource(objects_class=FakeObjectList([1, 2, 3]), max_limit=2)
        resource.get_object_list = Mock(return_value=[1, 2])
        resource.on_get(req=req, resp=resp)
        self.assertEqual(
            req.context['result'],
            {
                'results': [1, 2],
                'total': 3,
                'returned': 2
            }
        )
