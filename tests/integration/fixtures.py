import os
import shutil
import tempfile
from typing import List
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from cfdb.models.schema import Base
import pytest


@pytest.fixture
def sqlite_in_memory():
    """whether to create a sqlite DB in memory or on the filesystem."""
    return True


@pytest.fixture
def sqlite_url(sqlite_in_memory):
    if sqlite_in_memory:
        yield "sqlite:///:memory:"
    else:
        sql_path = tempfile.TemporaryDirectory()
        try:
            yield f"sqlite:///{sql_path.name}/test_database.sqlite"
        finally:
            sql_path.cleanup()


@pytest.fixture
def database_url(sqlite_url):
    db_url = os.environ.get("CF_TEST_DATABASE", sqlite_url)
    return db_url


@pytest.fixture
def sql_echo():
    """whether to activate SQL echo during the tests or not."""
    return False


@pytest.fixture
def engine(database_url, sql_echo):
    sql_echo = bool(os.environ.get("TEST_ECHO_SQL", sql_echo))
    engine = create_engine(database_url, echo=sql_echo, reuse_engine=False)
    yield engine
    engine.dispose()


@pytest.fixture
def create_tables(engine):
    Base.metadata.create_all(engine)


@pytest.fixture
def auto_rollback():
    """
    Enables automatic reverting of database changes after each test.
    Set to True for most tests, but use with caution. If set to False,
    manual cleanup of database objects is required after each test.
    """

    return True


@pytest.fixture
def sql_connection(engine):
    connection = engine.connect()
    yield connection
    connection.close()


@pytest.fixture
def session_maker(sql_connection, create_tables, auto_rollback):
    trans = None
    if auto_rollback:
        trans = sql_connection.begin()

    sql_connection.name = "sqlite-test"
    yield sessionmaker(bind=sql_connection)


@pytest.fixture
def db(session_maker):
    session = session_maker()
    yield session
    session.close()
