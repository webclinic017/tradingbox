import os
from databases import Database
import sqlalchemy

from settings.settings import DATABASE_HOST, DATABASE_PORT, DATABASE_USER, DATABASE_PASSWORD, DATABASE_NAME

def get_url_from_profile():
    if not (DATABASE_HOST and
            DATABASE_PORT and
            DATABASE_USER and
            DATABASE_PASSWORD and
            DATABASE_NAME):
        raise Exception('Bad config file! ')

    database_name = DATABASE_NAME
    return 'postgresql://{user}:{passwd}@{host}:{port}/{db}'.format(
        user=DATABASE_USER, passwd=DATABASE_PASSWORD, host=DATABASE_HOST, port=DATABASE_PORT, db=database_name)


DATABASE_URL = get_url_from_profile()

engine = sqlalchemy.create_engine(DATABASE_URL)

metadata = sqlalchemy.MetaData()

database = Database(DATABASE_URL)
