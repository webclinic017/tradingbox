import time
import os
import asyncio
import pandas as pd
import numpy as np

import models

from api.db_api import metadata, engine

from etl.etl_base import SqlTarget

from settings.settings import logger, LUIGI_CONFIG_PATH

import luigi
luigi.configuration.add_config_path(LUIGI_CONFIG_PATH)

########################################################################################################################
#                                   INITIALIZE DATABASE
#


class CreateDB(luigi.Task):
    priority = 100
    task_complete = False

    drop = luigi.BoolParameter(default=False)

    def complete(self):
        # Make sure you return false when you want the task to run.
        # And true when complete
        return self.task_complete

    def run(self):


        if self.drop:
            metadata.drop_all(engine, checkfirst=False)
        """
        for name, table in TABLES.items():
            if 'yahoo' in name:
                table.__table__.drop(db)
        """
        # Create database from SQLAlchemy models
        metadata.create_all(engine)

        self.task_complete = True


class LoadInitializeTask(luigi.Task):
  #  priority = 90

    CSV_TABLES = luigi.configuration.get_config().get('table', 'CSV_TABLES', None)
    PATH_CSV_TABLES = luigi.configuration.get_config().get('table', 'PATH_CSV_TABLES', None)
    task_complete = False

    def requires(self):
        return [CreateDB()]

    def complete(self):
        # Make sure you return false when you want the task to run.
        # And true when complete
        return self.task_complete

    def run(self):
        """
        The main function that processes and loads the auxiliary data into
        the database. For each table listed in the tables_to_load list, their
        CSV file is loaded and the data moved into the SQL database. If the
        table is for indices, the CSV data is passed to the find_symbol_id
        function, where the ticker is replaced with it's respective symbol_id.

        :return: Nothing. Data is just loaded into the SQL database.
        """

        start_time = time.time()
        tables = metadata.tables

        for name, table in tables.items():
            # if hasattr(table, '__tablename__'):
            #    tablenames.append(table.__tablename__)
            if name in self.CSV_TABLES.split(','):
                try:
                    file = os.path.abspath(os.path.join(self.PATH_CSV_TABLES, name + '.csv'))
                    # Read file into dataframe
                    csv_data = pd.read_csv(file, encoding='ISO-8859-1')
                    # Drop None element in dataframe
                    # csv_data = csv_data.where(pd.notnull(csv_data), None)
                    csv_data = csv_data.replace({np.nan: None})
                    csv_data.fillna("", inplace=True)
                    # csv_data = csv_data.where(csv_data != "", None)
                    csv_data = csv_data.replace({"": None})
                    csv_data = csv_data.replace({np.nan: None})

                except Exception as e:
                    logger.error('Unable to load the %s csv load file. Skipping it' % name.upper())
                    logger.error(str(e))
                    continue

                asyncio.run(self.output().load_data(data=csv_data, table_name=name))

                logger.info('List of all record for %s table created and merged on database' % name.upper())

        load_tables_excluded = [table for table in self.CSV_TABLES.split(',')
                                if table not in tables.keys()]
        if load_tables_excluded:
            logger.info('Unable to load the following tables: %s' % (", ".join(load_tables_excluded)))
            logger.info("If the CSV file exists, make sure it's name matches the name in the tables dictionary.")

        logger.info('Finished loading all selected tables taking %0.1f seconds' % (time.time() - start_time))

        self.task_complete = True

    def output(self):
        return SqlTarget()
