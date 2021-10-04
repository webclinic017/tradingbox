from luigi import Target

from settings.settings import logger
from api.db_api import database

from models import TABLES

class SqlTarget(Target):

    def exists(self):
        return False

    async def load_data(self, data, table_name):
        await database.connect()
        table = TABLES[table_name]
        if len(data) > 0:
            for idx, row in data.iterrows():
                try:
                    # create records to add
                    record = table.process_values(row)
                except Exception as e:
                    logger.error('Unable to create a record for %s table. Skipping it' % table_name.upper())
                    logger.error(str(e))
                    continue

                try:
                    await table.create_or_update(record)  # self.session.merge(record)
                except Exception as e:
                    logger.error('Job: Load Data on db - Something went wrong: %s' % record)
                    logger.error(str(e))
                    continue

                if idx > 0 and idx % 100 == 0:
                   # session.commit()  # self.session.commit()
                    logger.info('Job: Load Data on table %s - Chunked commit at %s records' % (table_name.upper(), idx))
            #session.commit()  # self.session.commit()
            #session.close()
        await database.disconnect()
        logger.info('Job: Load Data on db - %s records merged on %s table' % (len(data), table_name.upper()))
    #            self.session.close()

"""
class ExtractTableTask(luigi.Task):
    table_name = luigi.Parameter()
    start_date = luigi.DateParameter(default=None)
    end_date = luigi.DateParameter(default=None)

    PATH_CSV_TABLES = luigi.configuration.get_config().get('table', 'PATH_CSV_TABLES', None)
    data_dir = luigi.configuration.get_config().get('table', 'PATH_DOWNLOAD_FILE', None)

    def output(self):
        return luigi.LocalTarget(f'{self.data_dir}/{str(self.table_name)}.csv')

    def run(self):
        with self.output().temporary_path() as path:
            logger.info(f'Extracting {self.table_name} to {path}')
            try:
                file = os.path.abspath(os.path.join(self.PATH_CSV_TABLES, str(self.table_name) + '.csv'))
                # Read file into dataframe
                csv_data = pd.read_csv(file, encoding='ISO-8859-1')
                csv_data.where(csv_data.notnull(), None)
                # Convert dataframe to list and store in same variable
            #    csv_data = csv_data.values.tolist()
            except Exception as e:
                logger.error('Unable to load the %s csv load file. Skipping it' % str(self.table_name).upper())
                logger.error(str(e))


class LoadTask(ABC, luigi.Task):
    logger = setup_logging('luigi-interface')
    session = SessionLocal_Base()

    #start_date = datetime.date.today()#luigi.DateParameter(default=datetime.date.today())
    #end_date = datetime.date.today()#luigi.DateParameter(default=datetime.date.today())
    data_dir = luigi.configuration.get_config().get('table', 'PATH_DOWNLOAD_FILE', None)
    table_name = luigi.Parameter()

    def run(self):
        input_value = self.input()
        input_value = [input_value] if type(input_value) is LocalTarget else input_value
        for table_in in input_value:
            if isinstance(table_in, LocalTarget):
                in_file = table_in.path
                df = pd.read_csv(in_file)
                logger.info(f'loaded {len(df)} rows of CSV data from {in_file}')

                df_final = self.process_data(df)

                self.load_data(df_final)

        self.output().done()

    def output(self):
        target = None
        input_value = self.input()
        input_value = [input_value] if type(input_value) is LocalTarget else input_value
        for table_in in input_value:
            if isinstance(table_in, LocalTarget):
                # noinspection PyTypeChecker
                target = BatchStatusTarget(self.get_workflow_name(), self.start_date, self.end_date, table_in)

        return target

    def load_data(self, df) -> None:
        table = TABLES[self.table_name]
        for idx, row in df.iterrows():
            try:
                # create records to add
                record = table.process_values(row)
            except Exception:
                logger.error('Unable to create a record for %s table. Skipping it' % self.table_name.upper())
                logger.error(str(e))
                continue
            try:
                self.session.merge(record)
            except Exception:
                logger.info('Job: Load Data on db - Something went wrong: %s' % record)
                logger.error(str(e))
                continue

            logger.debug(record)
            if idx > 0 and idx % 100 == 0:
                self.session.commit()
                logger.info('Job: Load Data on db - Chunked commit at %s records' % idx)
        self.session.commit()
        logger.info('Job: Load Data on db - Chunked commit at %s records' % len(df))
#            self.session.close()

    @abstractmethod
    def process_data(self, data):
        pass

    @abstractmethod
    def get_workflow_name(self):
        pass
"""

###################################################################################################################
