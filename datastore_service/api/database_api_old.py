import asyncio

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.pool import NullPool

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from settings.settings import DATABASE_NAME, DATABASE_USER, DATABASE_PASSWORD
from settings.settings import DATABASE_HOST, DATABASE_PORT, logger

#SessionLocal_Base = scoped_session(sessionmaker())
# SessionLocal_App = scoped_session(sessionmaker())
SessionLocal_Base = sessionmaker()


def get_url_from_profile(db_type=None):
    if not (DATABASE_HOST and
            DATABASE_PORT and
            DATABASE_USER and
            DATABASE_PASSWORD and
            DATABASE_NAME):
        raise Exception('Bad config file! ')
    if db_type == "webappDB":
        database_name = DATABASE_NAME_APP
    else:
        database_name = DATABASE_NAME
    return 'postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(
        user=DATABASE_USER, passwd=DATABASE_PASSWORD, host=DATABASE_HOST, port=DATABASE_PORT, db=database_name)


def get_engine(db, user, host, port, passwd):
    """
    Get SQLalchemy engine using credentials.
    Input:
    db: database name
    user: Username
    host: Hostname of the database server
    port: Port number
    passwd: Password for the database
    """
    url = 'postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(
        user=user, passwd=passwd, host=host, port=port, db=db)
    engine = create_engine(url, pool_size = 50, echo_pool=True, executemany_mode='values')
    return engine


def get_connection_for_base_db():#config_file_name="settings.yaml"):
    """
    Sets up database connection from config file.
    """
#    with open(config_file_name, 'r') as f:
#        vals = yaml.safe_load(f)
    if not (DATABASE_HOST and
            DATABASE_PORT and
            DATABASE_USER and
            DATABASE_PASSWORD and
            DATABASE_NAME):
        raise Exception('Bad config file! ')
    return get_engine(DATABASE_NAME, DATABASE_USER, DATABASE_HOST, DATABASE_PORT, DATABASE_PASSWORD)


def get_connection_for_app_db():#config_file_name="settings.yaml"):
    """
    Sets up database connection from config file.
    """
#    with open(config_file_name, 'r') as f:
#        vals = yaml.safe_load(f)
    if not (DATABASE_HOST and
            DATABASE_PORT and
            DATABASE_USER and
            DATABASE_PASSWORD and
            DATABASE_NAME_APP):
        raise Exception('Bad config file! ')
    return get_engine(DATABASE_NAME_APP, DATABASE_USER, DATABASE_HOST, DATABASE_PORT, DATABASE_PASSWORD)

##############################################################################


def get_database(db_name="base"):
    try:
        if db_name == "app":
            engine = get_connection_for_app_db()
        elif db_name == "base":
            engine = get_connection_for_base_db()
        else:
            db_name = "app"
            engine = get_connection_for_base_db()
        logger.info("Connected to PostgreSQL database - %s" % db_name.upper())
    except IOError:
        logger.exception("Failed to get database connection!")
        return None, 'fail'
    return engine


def create_session_base():
    engine_base = get_database(db_name="base")
    SessionLocal_Base.configure(autocommit=False, autoflush=True, expire_on_commit=False, bind=engine_base)


def create_session_app():
    engine_app = get_database(db_name="app")
    SessionLocal_App.configure(autocommit=False, autoflush=True, expire_on_commit=False, bind=engine_app)


# https://stackoverflow.com/questions/36090055/sqlalchemy-best-practices-when-how-to-configure-a-scoped-session
create_session_base()
create_session_app()


######################################################################################################################
#                   ASYNC SESSION

#SessionAsyncLocal = scoped_session(sessionmaker(class_=AsyncSession))


def get_async_engine(db, user, host, port, passwd):
    """
    Get SQLalchemy engine using credentials.
    Input:
    db: database name
    user: Username
    host: Hostname of the database server
    port: Port number
    passwd: Password for the database
    """
    url = 'postgresql+asyncpg://{user}:{passwd}@{host}:{port}/{db}'.format(
        user=user, passwd=passwd, host=host, port=port, db=db)
    engine = create_async_engine(url, future=True, echo_pool=True, poolclass=NullPool, executemany_mode='values') #pool_size = 50, max_overflow = 10)
    return engine


def get_async_connection_from_profile():#config_file_name="settings.yaml"):
    """
    Sets up database connection from config file.
    Input:
    config_file_name: File containing PGHOST, PGUSER,
                        PGPASSWORD, PGDATABASE, PGPORT, which are the
                        credentials for the PostgreSQL database
    """
#    with open(config_file_name, 'r') as f:
#        vals = yaml.safe_load(f)
    if not (DATABASE_HOST and
            DATABASE_PORT and
            DATABASE_USER and
            DATABASE_PASSWORD and
            DATABASE_NAME):
        raise Exception('Bad config file! ')
    return get_async_engine(DATABASE_NAME, DATABASE_USER,
                        DATABASE_HOST, DATABASE_PORT,
                        DATABASE_PASSWORD)


def get_async_database():
    try:
        engine = get_async_connection_from_profile()
        logger.info("Connected to PostgreSQL database!")
    except IOError:
        logger.exception("Failed to get database connection!")
        return None, 'fail'
    return engine


def create_async_session():
    as_engine = get_async_database()
    return scoped_session(sessionmaker(bind=as_engine, autoflush=True, expire_on_commit=True, class_=AsyncSession))


#async_engine = get_async_database()
#async_session = sessionmaker(async_engine, expire_on_commit=False, class_=AsyncSession)
