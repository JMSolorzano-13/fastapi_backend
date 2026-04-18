import logging
import os
import warnings

from click import echo
from sqlalchemy import MetaData, create_engine
from sqlalchemy.exc import SAWarning
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from chalicelib.new.config.infra import envars

DATABASE = {
    "HOST": envars.DB_HOST,
    "PORT": envars.DB_PORT,
    "NAME": envars.DB_NAME,
    "USER": envars.DB_USER,
    "PASSWORD": envars.DB_PASSWORD,
}
DATABASE_RO = {
    "HOST": envars.DB_HOST_RO,
    "PORT": envars.DB_PORT,
    "NAME": envars.DB_NAME,
    "USER": envars.DB_USER,
    "PASSWORD": envars.DB_PASSWORD,
}

connection_uri = "postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{NAME}".format(**DATABASE)
connection_uri_ro = "postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{NAME}".format(
    **DATABASE_RO
)

common_engine_connection_args = {
    "poolclass": NullPool,
    "echo": False,
    "pool_pre_ping": False,  # No necesario con NullPool
    "connect_args": {
        # "connect_timeout": 3,
        # "options": "-c statement_timeout=25000",
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
        # "application_name": "lambda_function",
    },
}


engine = create_engine(
    connection_uri,
    **common_engine_connection_args,
)
engine_ro = create_engine(
    connection_uri_ro,
    **common_engine_connection_args,
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
SessionLocalRo = sessionmaker(autocommit=False, autoflush=False, bind=engine_ro)

logging.getLogger("sqlalchemy.engine.Engine").setLevel(envars.DB_LOG_LEVEL)
warnings.filterwarnings("ignore", category=SAWarning)

meta = MetaData(engine)


def get_engine(read_only: bool = False):
    return engine_ro if read_only else engine


def get_session_maker(read_only: bool = False) -> sessionmaker:
    return SessionLocalRo if read_only else SessionLocal


def create_connection_uri(persists_tests: bool, persist_database: bool) -> str:
    if not persist_database:
        return connection_uri
    db_config = DATABASE.copy()
    if not persists_tests:
        db_config["NAME"] += "_test"
    return "postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{NAME}".format(**db_config)
