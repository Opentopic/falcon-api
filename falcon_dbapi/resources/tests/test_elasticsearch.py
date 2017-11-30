import json
from collections import OrderedDict

import pytest

from falcon_dbapi.resources.elasticsearch import CollectionResource, ElasticSearchMixin
from elasticsearch import Elasticsearch
from elasticsearch_dsl import DocType, InnerObjectWrapper, String, Integer, Nested
from elasticsearch_dsl import Search


class OtherModel(InnerObjectWrapper):
    id = Integer()
    name = String(fields={'raw': String(index='not_analyzed')})


class Model(DocType):
    id = Integer()
    name = String()

    other_models = Nested(doc_class=OtherModel, multi=True, properties={
        'id': Integer(),
        'name': String(fields={'raw': String(index='not_analyzed')}),
    })

    class Meta:
        index = 'models'

    def get_term_query(self, column_name, value, default_op='should'):
        tq = ElasticSearchMixin.get_match_query(value, default_op)
        if column_name is None:
            column_name = 'name'
        return {'match': {column_name: tq}}


@pytest.fixture()
def connection():
    return Elasticsearch('localhost')


@pytest.fixture(params=[
    ({'name__exact': 'value'},
     """{"term": {"name": "value"}}"""),
    ("""{"name__exact": "value",
         "id__gte": "20"}""",
     """{"bool": {"must": [{"term": {"name": "value"}},
                           {"range": {"id": {"gte": "20"}}}]}}"""),
    ("""{"other_models__name": "value"}""",
     """{"nested": {"path": "other_models", "query": {"term": {"other_models.name": "value"}}}}"""),
    ("""{"or": {"name": "value",
                 "id": 20}}""",
     """{"bool": {"should": [{"term": {"name": "value"}},
                             {"term": {"id": 20}}]}}"""),
    ("""{"or": {"other_models__name": "value",
                "other_models__id__notexact": "20"}}""",
     """{"nested": {"path": "other_models",
                    "query": {"bool": {"should": [{"term": {"other_models.name": "value"}},
                                                  {"bool": {"must_not": [{"term": {"other_models.id": "20"}}]}}]}}}}"""),  # noqa
    ("""{"or": [{"name": "value",
                  "id": 20},
                {"name": "value2",
                  "id": 10},
                {"and": {"name": "value3",
                         "id": 0}}]}""",
     """{"bool": {"should": [{"bool": {"must": [{"term": {"name": "value"}},
                                                {"term": {"id": 20}}]}},
                             {"bool": {"must": [{"term": {"name": "value2"}},
                                                {"term": {"id": 10}}]}},
                             {"bool": {"must": [{"term": {"name": "value3"}},
                                                {"term": {"id": 0}}]}}]}}"""),
    ("""{"and": [{"or": {"name": "value",
                         "id": 20}},
                 {"or": {"name": "value3",
                         "id": 0}}]}""",
     """{"bool": {"must": [{"bool": {"should": [{"term": {"name": "value"}},
                                                {"term": {"id": 20}}]}},
                           {"bool": {"should": [{"term": {"name": "value3"}},
                                                {"term": {"id": 0}}]}}]}}"""),
    ("""{"not": {"other_models__name__isnull": "true"}}""",
     """{"bool": {"must_not": [{"nested": {"path": "other_models",
                                           "query": {"missing": {"field": "other_models.name"}}}}]}}"""),
    ("""{"and": {"q": "value2",
                 "q__or": ["value", "value2"],
                 "q__and": ["one", "two", "three"]}}""",
     """{"bool": {"must": [{"match": {"name": {"operator": "and", "boost": 1, "query": "value2"}}},
                           {"match": {"name": {"operator": "or",
                                               "boost": 1,
                                               "query": "\\"value\\" \\"value2\\""}}},
                           {"match": {"name": {"operator": "and",
                                               "boost": 1,
                                               "query": "\\"one\\" \\"two\\" \\"three\\""}}}]}}"""),
])
def query_filtered(request):
    return request.param


@pytest.fixture(params=[
    (['-name', 'id'],
     """{"query": {"match_all": {}}, "sort": [{"name": {"order": "desc"}}, "id"]}"""),
])
def query_ordered(request):
    return request.param


@pytest.fixture(params=[
    ("""[{"sum": ["id"]}]""",
     """{"aggs": {"sum": {"sum": {"field": "id"}}},
         "query": {"match_all": {}}}"""),

    ("""[{"count": ["id"]},
         {"group_by": ["name"]}]""",
     """{"aggs": {"name": {"terms": {"field": "name",
                                     "size": 0}}},
         "query": {"match_all": {}}}"""),

    ("""[{"sum": ["id"]},
         {"group_by": ["name"]}]""",
     """{"aggs": {"name": {"terms": {"field":
                                     "name", "size": 0,
                                     "order": {"sum": "desc"}},
                           "aggs": {"sum": {"sum": {"field": "id"}}}}},
         "query": {"match_all": {}}}"""),

    ("""[{"sum": ["id"]},
         {"group_by": ["name"]},
         {"group_limit": 5}]""",
     """{"aggs": {"name": {"terms": {"field": "name",
                                     "size": 5,
                                     "order": {"sum": "desc"}},
                           "aggs": {"sum": {"sum": {"field": "id"}}}}},
         "query": {"match_all": {}}}"""),

    ("""[{"sum": ["id"]},
         {"group_by": ["other_models__name"]},
         {"group_limit": 5}]""",
     """{"aggs": {"nested": {"nested": {"path": "other_models"},
                             "aggs": {"other_models__name": {"terms": {"field": "other_models.name.raw",
                                                                       "size": 5,
                                                                       "order": {"sum": "desc"}},
                                                             "aggs": {"sum": {"sum": {"field": "id"}}} }} }},
         "query": {"match_all": {}}}"""),

    ("""[{"max": ["other_models__id"]},
         {"group_by": [{"other_models__id__gte": 5}, "other_models__name"]}]""",
     """{"aggs": {"nested": {"nested": {"path": "other_models"},
                             "aggs": {"filtered": {"filter": {"range": {"other_models.id": {"gte": 5}}},
                                                   "aggs": {"other_models__name": {"terms": {"field": "other_models.name.raw",
                                                                                             "size": 0,
                                                                                             "order": {"max": "desc"}},
                                                                                   "aggs": {"max": {"max": {"field": "other_models.id"}}} }} }} }},
         "query": {"match_all": {}}}"""),  # noqa
])
def query_totals(request):
    return request.param


@pytest.fixture()
def model():
    model1 = Model()
    model1.id = 1
    model1.name = 'model'
    model1.other_models.append({'id': 2, 'name': 'other_model1'})
    model1.other_models.append({'id': 3, 'name': 'other_model2'})
    return model1


def test_filter_by(connection, query_filtered):
    """
    Test `get_object` func
    """
    conditions, expected = query_filtered
    if isinstance(conditions, str):
        conditions = json.loads(conditions, object_pairs_hook=OrderedDict)
    if isinstance(expected, str):
        expected = json.loads(expected)
    c = CollectionResource(objects_class=Model, connection=connection)
    query_obj = c.filter_by(Search(using=connection).doc_type(Model), conditions)
    assert query_obj.to_dict()['query'] == expected


def test_order_by(connection, query_ordered):
    """
    Test `get_object` func
    """
    conditions, expected = query_ordered
    if isinstance(conditions, str):
        conditions = json.loads(conditions, object_pairs_hook=OrderedDict)
    if isinstance(expected, str):
        expected = json.loads(expected)
    query_obj = Search(using=connection, doc_type=Model).sort(*conditions)
    assert query_obj.to_dict() == expected


def test_totals(connection, query_totals):
    """
    Test `get_object` func
    """
    totals, expected = query_totals
    if isinstance(totals, str):
        totals = json.loads(totals, object_pairs_hook=OrderedDict)
    if isinstance(expected, str):
        expected = json.loads(expected)
    c = CollectionResource(objects_class=Model, connection=connection)
    query_obj = c._build_total_expressions(Search(using=connection).doc_type(Model), totals)
    assert query_obj.to_dict() == expected


def test_flatten_aggregates():
    value = """
{"buckets":[
  {"key_as_string":"2017-02-27T00:00:00.000Z",
   "key":1490372800000,
   "doc_count":1,
   "nested":{"doc_count":15,
             "avg":{"value":0.3355697842935721}}},
  {"key_as_string":"2017-03-27T00:00:00.000Z",
   "key":1490572800000,
   "doc_count":1,
   "nested":{"doc_count":12,
             "avg":{"value":0.4355697842935721}}}
]}
"""
    result_key, result_value = CollectionResource.flatten_aggregate('foo', json.loads(value))
    assert result_key == 'avg'
    assert result_value == {'2017-02-27T00:00:00.000Z': 0.3355697842935721,
                            '2017-03-27T00:00:00.000Z': 0.4355697842935721}
