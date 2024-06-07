from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from refactored_code import metadata, users, heart_rates, query_users, query_for_user

# Настройка тестовой базы данных
DATABASE_URL = 'postgresql://username:password@localhost:5432/test_database_name'
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)


@pytest.fixture(scope='module')
def setup_database():
    # Создание схемы в тестовой базе данных
    metadata.create_all(engine)
    yield
    metadata.drop_all(engine)


@pytest.fixture(scope='function')
def db_session(setup_database):
    session = Session()
    yield session
    session.close()


def insert_test_data(session):
    # Вставка тестовых данных
    user1_id = uuid4()
    user2_id = uuid4()
    session.execute(users.insert(), [
        {'id': user1_id, 'name': 'Alice', 'gender': 'female', 'age': 25},
        {'id': user2_id, 'name': 'Bob', 'gender': 'male', 'age': 30}
    ])
    now = datetime.now()
    session.execute(heart_rates.insert(), [
        {'id': uuid4(), 'user_id': user1_id, 'timestamp': int((now - timedelta(hours=1)).timestamp()),
         'heart_rate': 70},
        {'id': uuid4(), 'user_id': user1_id, 'timestamp': int((now - timedelta(hours=2)).timestamp()),
         'heart_rate': 75},
        {'id': uuid4(), 'user_id': user2_id, 'timestamp': int((now - timedelta(hours=1)).timestamp()),
         'heart_rate': 65},
        {'id': uuid4(), 'user_id': user2_id, 'timestamp': int((now - timedelta(hours=2)).timestamp()), 'heart_rate': 80}
    ])
    session.commit()


def test_query_users(db_session):
    insert_test_data(db_session)
    date_from = datetime.now() - timedelta(days=1)
    date_to = datetime.now()
    result = query_users(min_age=20, gender='female', min_avg_heart_rate=60, date_from=date_from, date_to=date_to)

    assert len(result) == 1
    assert result[0]['name'] == 'Alice'


def test_query_for_user(db_session):
    insert_test_data(db_session)
    user_id = db_session.execute(select(users.c.id).where(users.c.name == 'Alice')).scalar()
    date_from = datetime.now() - timedelta(days=1)
    date_to = datetime.now()
    result = query_for_user(user_id=user_id, date_from=date_from, date_to=date_to)

    assert len(result) == 2
    assert result[0]['avg_heart_rate'] > result[1]['avg_heart_rate']


if __name__ == "__main__":
    pytest.main()
