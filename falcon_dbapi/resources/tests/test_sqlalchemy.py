import json
from collections import OrderedDict

import pytest
from sqlalchemy.sql.elements import or_
from sqlalchemy.sql.functions import Function
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, Table
from sqlalchemy.orm import relationship

from falcon_dbapi.resources.sqlalchemy import CollectionResource, AlchemyMixin

Base = declarative_base()


m2m_table = Table('m2m_table', Base.metadata,
                  Column('model_id', ForeignKey('some_table.id', onupdate='CASCADE', ondelete='CASCADE'),
                         primary_key=True, index=True),
                  Column('other_model_id', ForeignKey('other_table.id', onupdate='CASCADE', ondelete='CASCADE'),
                         primary_key=True, index=True)
                  )


class Model(Base):
    __tablename__ = 'some_table'
    id = Column(Integer, primary_key=True)
    name = Column(String)

    other_models = relationship('OtherModel', secondary=m2m_table, back_populates='models')

    def get_term_query(self, column_alias, column_name, value, default_op=or_):
        tq = AlchemyMixin.get_tsquery(value, default_op)
        if column_name is None:
            column_name = column_alias.name
        return Function('to_tsvector', column_name).op('@@')(tq)


class OtherModel(Base):
    __tablename__ = 'other_table'
    id = Column(Integer, primary_key=True)
    name = Column(String)

    third_models = relationship('ThirdModel', back_populates='other_model')
    models = relationship('Model', secondary=m2m_table, back_populates='other_models')


class ThirdModel(Base):
    __tablename__ = 'third_table'
    id = Column(Integer, primary_key=True)
    other_model_id = Column(Integer, ForeignKey('other_table.id', onupdate='CASCADE', ondelete='RESTRICT'),
                            index=True, nullable=False)
    name = Column(String)

    other_model = relationship('OtherModel', back_populates='third_models')


class CompositeModel(Base):
    __tablename__ = 'composite_table'
    a_id = Column(Integer, primary_key=True)
    b_id = Column(Integer, primary_key=True)
    name = Column(String)


@pytest.fixture()
def engine():
    from sqlalchemy import create_engine
    return create_engine('sqlite:///:memory:', echo=True)


@pytest.fixture()
def session(request, engine):
    from sqlalchemy.orm import Session
    session = Session(engine)
    Base.metadata.create_all(engine)

    def fin():
        Base.metadata.drop_all(engine)
        session.close()
    request.addfinalizer(fin)
    return session


@pytest.fixture(params=[
    ({'name__exact': 'value'},
     """SELECT some_table.id, some_table.name %20
FROM some_table %20
WHERE some_table.name = ?"""),
    ("""{"name__exact": "value",
         "id__gte": 20}""",
     """SELECT some_table.id, some_table.name %20
FROM some_table %20
WHERE some_table.name = ? AND some_table.id >= ?"""),
    ("""{"other_models__name": "value",
         "other_models__third_models__name": "value"}""",
     """SELECT DISTINCT some_table.id, some_table.name %20
FROM some_table %0A
JOIN m2m_table AS m2m_table_1 ON some_table.id = m2m_table_1.model_id %0A
JOIN other_table AS other_models_1 ON other_models_1.id = m2m_table_1.other_model_id %0A
JOIN third_table AS third_models_1 ON other_models_1.id = third_models_1.other_model_id %20
WHERE other_models_1.name = ? AND third_models_1.name = ?"""),
    ("""{"or": {"name": "value",
                 "id": 20}}""",
     """SELECT some_table.id, some_table.name %20
FROM some_table %20
WHERE some_table.name = ? OR some_table.id = ?"""),
    ("""{"or": {"other_models__name": "value",
                "other_models__third_models__name": "value"}}""",
     """SELECT DISTINCT some_table.id, some_table.name %20
FROM some_table %0A
JOIN m2m_table AS m2m_table_1 ON some_table.id = m2m_table_1.model_id %0A
JOIN other_table AS other_models_1 ON other_models_1.id = m2m_table_1.other_model_id %0A
JOIN third_table AS third_models_1 ON other_models_1.id = third_models_1.other_model_id %20
WHERE other_models_1.name = ? OR third_models_1.name = ?"""),
    ("""{"or": [{"name": "value",
                  "id": 20},
                {"name": "value2",
                  "id": 10},
                {"and": {"name": "value3",
                         "id": 0}}]}""",
     """SELECT some_table.id, some_table.name %20
FROM some_table %20
WHERE some_table.name = ? AND some_table.id = ? OR some_table.name = ? AND some_table.id = ? %0A
OR some_table.name = ? AND some_table.id = ?"""),
    ("""{"and": [{"or": {"name": "value",
                         "id": 20}},
                 {"or": {"name": "value3",
                         "id": 0}}]}""",
     """SELECT some_table.id, some_table.name %20
FROM some_table %20
WHERE (some_table.name = ? OR some_table.id = ?) AND (some_table.name = ? OR some_table.id = ?)"""),
    ("""{"not": {"other_models__name__isnull": "true"}}""",
     """SELECT DISTINCT some_table.id, some_table.name %20
FROM some_table %0A
LEFT OUTER JOIN (m2m_table AS m2m_table_1 %0A
JOIN other_table AS other_models_1 ON other_models_1.id = m2m_table_1.other_model_id) %0A
ON some_table.id = m2m_table_1.model_id %20
WHERE other_models_1.name IS NOT NULL"""),
    ("""{"and": {"name__func__first_func__other_func": 20,
                 "name__func__single_func": 20,
                 "name__sfunc__noarg_func": null,
                 "name__efunc__id__greatest": null}}""",
     """SELECT some_table.id, some_table.name %20
FROM some_table %20
WHERE other_func(first_func(some_table.name), ?) AND single_func(some_table.name, ?) AND noarg_func(some_table.name) %0A
AND greatest(some_table.name, some_table.id)"""),
    ("""{"and": {"q": "value2",
                 "q__or": ["value", "value2"],
                 "q__and": ["one", "two", "three"]}}""",
     """SELECT some_table.id, some_table.name %20
FROM some_table %20
WHERE (to_tsvector(some_table.name) @@ plainto_tsquery(?, ?)) %0A
AND (to_tsvector(some_table.name) @@ (plainto_tsquery(?, ?) || plainto_tsquery(?, ?))) %0A
AND (to_tsvector(some_table.name) @@ ((plainto_tsquery(?, ?) && plainto_tsquery(?, ?)) && plainto_tsquery(?, ?)))"""),
])
def query_filtered(request):
    return request.param


@pytest.fixture(params=[
    (['-name', 'id'],
     """SELECT some_table.id, some_table.name %20
FROM some_table %0A
ORDER BY some_table.name DESC, some_table.id"""),
    ({'-other_models__name__func__jsonb_object_field_text': 'value'},
     """SELECT some_table.id, some_table.name %20
FROM some_table %0A
JOIN m2m_table AS m2m_table_1 ON some_table.id = m2m_table_1.model_id %0A
JOIN other_table AS other_models_1 ON other_models_1.id = m2m_table_1.other_model_id %0A
ORDER BY jsonb_object_field_text(other_models_1.name, ?) DESC"""),
])
def query_ordered(request):
    return request.param


@pytest.fixture(params=[
    ("""[{"sum": ["other_models__id"]}]""",
     """SELECT sum(totals_other_models_1.id) AS sum %20
FROM some_table %0A
JOIN m2m_table AS m2m_table_1 ON some_table.id = m2m_table_1.model_id %0A
JOIN other_table AS totals_other_models_1 ON totals_other_models_1.id = m2m_table_1.other_model_id %0A
ORDER BY 1 DESC"""),
    ("""[{"count": ["other_models__id"]},
         {"group_by": ["other_models__name"]}]""",
     """SELECT totals_other_models_1.name AS other_models__name, count(totals_other_models_1.id) AS count %20
FROM some_table %0A
JOIN m2m_table AS m2m_table_1 ON some_table.id = m2m_table_1.model_id %0A
JOIN other_table AS totals_other_models_1 ON totals_other_models_1.id = m2m_table_1.other_model_id %0A
GROUP BY totals_other_models_1.name %0A
ORDER BY 1,2 DESC"""),
    ("""[{"count": ["other_models__id"]},
         {"group_by": ["other_models__name"]},
         {"group_limit": 5}]""",
     """SELECT anon_1.other_models__name, anon_1.count, anon_1.row_number %20
FROM (SELECT totals_other_models_1.name AS other_models__name, count(totals_other_models_1.id) AS count, %0A
row_number() OVER (ORDER BY count(totals_other_models_1.id) DESC) AS row_number %20
FROM some_table %0A
JOIN m2m_table AS m2m_table_1 ON some_table.id = m2m_table_1.model_id %0A
JOIN other_table AS totals_other_models_1 ON totals_other_models_1.id = m2m_table_1.other_model_id %0A
GROUP BY totals_other_models_1.name %0A
ORDER BY 1,2 DESC) AS anon_1 %20
WHERE anon_1.row_number <= ?"""),
])
def query_totals(request):
    return request.param


@pytest.fixture()
def model():
    model1 = Model()
    model1.id = 1
    model1.name = 'model'
    other_model1 = OtherModel()
    other_model1.id = 2
    other_model1.name = 'other_model1'
    other_model2 = OtherModel()
    other_model2.id = 3
    other_model2.name = 'other_model2'
    third_model1 = ThirdModel()
    third_model1.id = 4
    third_model1.name = 'third_model1'
    third_model2 = ThirdModel()
    third_model2.id = 5
    third_model2.name = 'third_model2'
    other_model1.third_models = [third_model1, third_model2]
    model1.other_models = [other_model1, other_model2]
    return model1


def test_filter_by(engine, session, query_filtered):
    """
    Test `get_object` func
    """
    conditions, expected = query_filtered
    if isinstance(conditions, str):
        conditions = json.loads(conditions, object_pairs_hook=OrderedDict)
    c = CollectionResource(objects_class=Model, db_engine=engine)
    query_obj = c._filter_or_exclude(session.query(Model), conditions)
    assert str(query_obj.statement.compile(engine)) == expected.replace(' %20', ' ').replace(' %0A\n', ' ')


def test_order_by(engine, session, query_ordered):
    """
    Test `get_object` func
    """
    conditions, expected = query_ordered
    if isinstance(conditions, str):
        conditions = json.loads(conditions, object_pairs_hook=OrderedDict)
    c = CollectionResource(objects_class=Model, db_engine=engine)
    query_obj = c.order_by(session.query(Model), conditions)
    assert str(query_obj.statement.compile(engine)) == expected.replace(' %20', ' ').replace(' %0A\n', ' ')


def test_totals(engine, session, query_totals):
    """
    Test `get_object` func
    """
    conditions, expected = query_totals
    if isinstance(conditions, str):
        conditions = json.loads(conditions, object_pairs_hook=OrderedDict)
    c = CollectionResource(objects_class=Model, db_engine=engine)
    stmt, _ = c._build_total_expressions(session.query(Model), conditions)
    assert str(stmt.compile(engine)) == expected.replace(' %20', ' ').replace(' %0A\n', ' ')


def test_composite(engine, session):
    """
    Test `get_object` func
    """
    c = CollectionResource(objects_class=CompositeModel, db_engine=engine)
    stmt, _ = c._build_total_expressions(session.query(CompositeModel), [{"count": None}])
    expected = """SELECT count(row(composite_table.a_id, composite_table.b_id)) AS count %20
FROM composite_table ORDER BY 1 DESC"""
    assert str(stmt.compile(engine)) == expected.replace(' %20', ' ').replace(' %0A\n', ' ')


def test_serialize(model):
    alchemy = AlchemyMixin()
    expected = {
        'id': 1,
        'name': 'model',
        'other_models': [
            {
                'id': 2,
                'name': 'other_model1',
            },
            {
                'id': 3,
                'name': 'other_model2',
            },
        ],
    }
    assert alchemy.serialize(model) == expected


def test_serialize_deep(model):
    alchemy = AlchemyMixin()
    expected = {
        'id': 1,
        'name': 'model',
        'other_models': [
            {
                'id': 2,
                'name': 'other_model1',
                'third_models': [
                    {
                        'id': 4,
                        'name': 'third_model1',
                        'other_model_id': None,
                    },
                    {
                        'id': 5,
                        'name': 'third_model2',
                        'other_model_id': None,
                    },
                ],
            },
            {
                'id': 3,
                'name': 'other_model2',
                'third_models': [],
            },
        ],
    }
    assert alchemy.serialize(model, relations_level=2) == expected


def test_deserialize(model):
    alchemy = AlchemyMixin()
    alchemy.objects_class = Model
    data = {
        'id': 1,
        'name': 'model',
        'other_models': [
            {
                'id': 2,
                'name': 'other_model1',
            },
            {
                'id': 3,
                'name': 'other_model2',
            },
        ],
    }
    expected = {
        'id': 1,
        'name': 'model',
        'other_models': [
            {
                'id': 2,
                'name': 'other_model1',
            },
            {
                'id': 3,
                'name': 'other_model2',
            },
        ],
    }
    assert alchemy.deserialize(data) == expected


def test_default_schema():
    expected = {
        'type': 'object',
        'properties': {},
        'required': [],
    }
    assert AlchemyMixin.get_default_schema(Model, 'POST') == expected
    expected = {
        'type': 'object',
        'properties': {},
        'required': [],
    }
    assert AlchemyMixin.get_default_schema(ThirdModel, 'POST') == expected


def test_save_resource(session):
    alchemy = AlchemyMixin()
    data = {
        'name': 'model',
        'other_models': [
            {
                'name': 'other_model1',
                'third_models': [
                    {
                        'name': 'third_model1',
                    },
                    {
                        'name': 'third_model2',
                    },
                ],
            },
            {
                'name': 'other_model2',
            },
        ],
    }
    new_model = Model()
    alchemy.save_resource(new_model, data, session)
    existing_model = session.query(Model).filter(Model.name == 'model').first()
    assert existing_model.name == data['name']
    assert existing_model.other_models[0].name == data['other_models'][0]['name']
    assert existing_model.other_models[1].name == data['other_models'][1]['name']
    assert existing_model.other_models[0].third_models[0].name == data['other_models'][0]['third_models'][0]['name']
    assert existing_model.other_models[0].third_models[1].name == data['other_models'][0]['third_models'][1]['name']


def test_update_resource(model, session):
    # save the example model to the db
    session.add(model)
    session.commit()
    # update it so some related models should be added, updated and removed
    alchemy = AlchemyMixin()
    data = {
        'name': 'model_prim',
        'other_models': [
            {
                'id': 2,
                'name': 'other_model1_prim',
                # missing third_models, should be left intact
            },
            # missing other_model with id 3, should be removed
            {
                # new other_model, should be added
                'name': 'other_model3',
            },
        ],
    }
    alchemy.save_resource(model, data, session)
    session.commit()
    model = session.query(Model).get(1)
    assert model.name == 'model_prim'
    assert len(model.other_models) == 2
    assert model.other_models[0].name == 'other_model1_prim'
    assert model.other_models[1].id == 4
    assert model.other_models[1].name == 'other_model3'


def test_update_resource2(model, session):
    # save the example model to the db
    session.add(model)
    session.commit()
    # update it so some related models should be added, updated and removed
    alchemy = AlchemyMixin()
    other_model = session.query(OtherModel).get(2)
    data = {
        'name': 'other_model1_prim2',
        'third_models': [
            {
                'id': 4,
                'other_model_id': 2,
                'name': 'third_model1_prim',
            },
            # have to include second model even without change because otherwise it would get removed
            # and one to many objects cannot be disassociated (not null constraint on FK)
            {
                'id': 5,
                'other_model_id': 2,
                'name': 'third_model2',
            }
        ]
    }
    alchemy.save_resource(other_model, data, session)
    session.commit()
    other_model = session.query(OtherModel).get(2)
    assert other_model.name == 'other_model1_prim2'
    assert other_model.third_models[0].name == 'third_model1_prim'
    assert other_model.third_models[1].name == 'third_model2'
