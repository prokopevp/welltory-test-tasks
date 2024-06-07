from datetime import datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, ForeignKey, func, select, \
    Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.engine import Engine

# Настройка подключения к базе данных
DATABASE_URL = 'postgresql://username:password@host:port/database_name'
engine: Engine = create_engine(DATABASE_URL)
metadata = MetaData()

# Определение схемы таблиц
users = Table(
    'users',
    metadata,
    Column('id', UUID(as_uuid=True), primary_key=True, default=uuid4),
    Column('name', String, nullable=False),
    Column('gender', String, nullable=False),
    Column('age', Integer, nullable=False)  # Изменено на Integer
)

heart_rates = Table(
    'heart_rates',
    metadata,
    Column('id', UUID(as_uuid=True), primary_key=True, default=uuid4),
    Column('user_id', UUID(as_uuid=True), ForeignKey('users.id'), index=True, nullable=False),
    Column('timestamp', Integer, nullable=False),  # Изменено на Integer
    Column('heart_rate', Float, nullable=False),
)

metadata.create_all(engine)


def query_users(
        min_age: int,
        gender: str,
        min_avg_heart_rate: float,
        date_from: datetime,
        date_to: datetime
) -> list[dict[str, Any]]:
    """
    Выполняет запрос для получения всех пользователей старше заданного возраста,
    с заданным полом и средним пульсом выше указанного значения в определенном временном промежутке.

    :param min_age: Минимальный возраст пользователей
    :param gender: Пол пользователей
    :param min_avg_heart_rate: Минимальный средний пульс
    :param date_from: Начало временного промежутка
    :param date_to: Конец временного промежутка
    :return: Список пользователей, удовлетворяющих условиям запроса
    """
    date_from_ts = int(date_from.timestamp())
    date_to_ts = int(date_to.timestamp())

    stmt = select(users).where(
        users.c.age > min_age,
        users.c.gender == gender,
        users.c.id.in_(
            select(heart_rates.c.user_id)
            .where(
                heart_rates.c.timestamp.between(date_from_ts, date_to_ts)
            )
            .group_by(heart_rates.c.user_id)
            .having(func.avg(heart_rates.c.heart_rate) > min_avg_heart_rate)
        )
    )

    with engine.connect() as connection:
        results = connection.execute(stmt).mappings().fetchall()
        return [dict(row) for row in results]


def query_for_user(
        user_id: UUID,
        date_from: datetime,
        date_to: datetime
) -> list[dict[str, Any]]:
    """
    Выполняет запрос для получения топ 10 самых высоких средних показателей пульса за часовые промежутки
    в указанном периоде для заданного пользователя.

    :param user_id: ID пользователя
    :param date_from: Начало временного промежутка
    :param date_to: Конец временного промежутка
    :return: Список 10 часовых промежутков с самыми высокими средними показателями пульса
    """
    date_from_ts = int(date_from.timestamp())
    date_to_ts = int(date_to.timestamp())

    subquery = select(
        func.date_trunc('hour', func.to_timestamp(heart_rates.c.timestamp)).label('hour'),
        func.avg(heart_rates.c.heart_rate).label('avg_heart_rate')
    ).where(
        heart_rates.c.user_id == user_id,
        heart_rates.c.timestamp.between(date_from_ts, date_to_ts)
    ).group_by(
        func.date_trunc('hour', func.to_timestamp(heart_rates.c.timestamp))
    ).subquery()

    stmt = select(
        subquery.c.hour,
        subquery.c.avg_heart_rate
    ).order_by(
        subquery.c.avg_heart_rate.desc()
    ).limit(10)

    with engine.connect() as connection:
        results = connection.execute(stmt).mappings().fetchall()
        return [dict(row) for row in results]
