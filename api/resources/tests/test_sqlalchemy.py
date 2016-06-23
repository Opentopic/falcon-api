import json
from collections import OrderedDict

import pytest
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, Table
from sqlalchemy.orm import relationship

from api.resources.sqlalchemy import CollectionResource

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
    return create_engine('sqlite:///:memory:')


@pytest.fixture()
def session(request, engine):
    from sqlalchemy.orm import Session
    session = Session(engine)

    def fin():
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
])
def query(request):
    return request.param


def test_filter_by(engine, session, query):
    """
    Test `get_object` func
    """
    conditions, expected = query
    if isinstance(conditions, str):
        conditions = json.loads(conditions, object_pairs_hook=OrderedDict)
    c = CollectionResource(objects_class=Model, db_engine=engine)
    query_obj = c._filter_or_exclude(session.query(Model), conditions)
    assert str(query_obj.statement.compile(engine)) == expected.replace(' %20', ' ').replace(' %0A\n', ' ')
