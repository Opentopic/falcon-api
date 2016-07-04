import json
from collections import OrderedDict

import pytest
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, Table
from sqlalchemy.orm import relationship

from api.resources.sqlalchemy import CollectionResource, AlchemyMixin

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
JOIN other_table AS other_table_1 ON other_table_1.id = m2m_table_1.other_model_id %0A
JOIN third_table AS third_table_1 ON other_table_1.id = third_table_1.other_model_id %20
WHERE other_table_1.name = ? AND third_table_1.name = ?"""),
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
JOIN other_table AS other_table_1 ON other_table_1.id = m2m_table_1.other_model_id %0A
JOIN third_table AS third_table_1 ON other_table_1.id = third_table_1.other_model_id %20
WHERE other_table_1.name = ? OR third_table_1.name = ?"""),
    ("""{"or": [{"name": "value",
                  "id": 20},
                {"name": "value2",
                  "id": 10},
                {"and": {"name": "value3",
                         "id": 0}}]}""",
     """SELECT some_table.id, some_table.name %20
FROM some_table %20
WHERE some_table.name = ? AND some_table.id = ? OR some_table.name = ? AND some_table.id = ? OR some_table.name = ? AND some_table.id = ?"""),  # noqa
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
JOIN other_table AS other_table_1 ON other_table_1.id = m2m_table_1.other_model_id) %0A
ON some_table.id = m2m_table_1.model_id %20
WHERE other_table_1.name IS NOT NULL"""),
    ("""{"and": {"name__func__first_func__other_func": 20,
                 "name__func__single_func": 20}}""",
     """SELECT some_table.id, some_table.name %20
FROM some_table %20
WHERE other_func(first_func(some_table.name), ?) AND single_func(some_table.name, ?)"""),
])
def query_filtered(request):
    return request.param


@pytest.fixture(params=[
    (['-name', 'id'],
     """SELECT some_table.id, some_table.name %20
FROM some_table %0A
ORDER BY some_table.name DESC, some_table.id"""),
    ({'-other_models__name__func__jsonb_object_field_text': 'value'},
     """SELECT DISTINCT some_table.id, some_table.name, %0A
jsonb_object_field_text(other_table_1.name, ?) AS jsonb_object_field_text_1 %20
FROM some_table %0A
JOIN m2m_table AS m2m_table_1 ON some_table.id = m2m_table_1.model_id %0A
JOIN other_table AS other_table_1 ON other_table_1.id = m2m_table_1.other_model_id %0A
ORDER BY jsonb_object_field_text(other_table_1.name, ?) DESC"""),
])
def query_ordered(request):
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


def test_serialize(model):
    alchemy = AlchemyMixin()
    expected = {
        'id': 1,
        'name': 'model',
        'other_models': {
            2: {
                'name': 'other_model1',
            },
            3: {
                'name': 'other_model2',
            },
        },
    }
    assert alchemy.serialize(model) == expected


def test_serialize_deep(model):
    alchemy = AlchemyMixin()
    expected = {
        'id': 1,
        'name': 'model',
        'other_models': {
            2: {
                'name': 'other_model1',
                'third_models': {
                    4: {
                        'name': 'third_model1',
                        'other_model_id': None,
                    },
                    5: {
                        'name': 'third_model2',
                        'other_model_id': None,
                    },
                },
            },
            3: {
                'name': 'other_model2',
                'third_models': {}
            },
        },
    }
    assert alchemy.serialize(model, relations_level=2) == expected


def test_deserialize(model):
    alchemy = AlchemyMixin()
    alchemy.objects_class = Model
    data = {
        'id': 1,
        'name': 'model',
        'other_models': {
            2: {
                'name': 'other_model1',
            },
            3: {
                'name': 'other_model2',
            },
        },
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
