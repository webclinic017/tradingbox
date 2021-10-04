import asyncio
import luigi
from luigi import LocalTarget

import pandas as pd
import numpy as np

from models.base import SubExchanges, Symbology

from etl.utils.build_symbology import create_symbology
from etl.utils.build_subexchange import create_subexchange

from etl.load_initialize import LoadInitializeTask
from etl.etl_extrator import ExtractorCSIDataTask, ExtractorBorsaItalianaTask
from etl.etl_base import SqlTarget

from settings.config import DATA_TYPE_STOCK
from settings.settings import logger


from settings.settings import LUIGI_CONFIG_PATH

luigi.configuration.add_config_path(LUIGI_CONFIG_PATH)


nothing = [None, np.nan, 'None', 'n/a', 'none', 'NONE']


########################################################################################################################
#                                           MAIN FUCTION


class LoadDataSymbologyTask(luigi.Task):
 #   priority = 80

    csidata_type = DATA_TYPE_STOCK
    # csidata_type = configuration.get_config().get('data_extractor', 'csidata_type', None)
    symbology_sources = luigi.configuration.get_config().get('data_extractor', 'symbology_sources', None)
    data_dir = luigi.configuration.get_config().get('table', 'PATH_DOWNLOAD_FILE', None)

    task_complete = False

    def complete(self):
        # Make sure you return false when you want the task to run.
        # And true when complete
        return self.task_complete

    def requires(self):
        return [LoadInitializeTask(),
                ExtractorCSIDataTask(self.csidata_type),
                ExtractorBorsaItalianaTask('azioni'),
                ExtractorBorsaItalianaTask('etf'),
                ExtractorBorsaItalianaTask('etc-etn'),
                ExtractorBorsaItalianaTask('fondi'),
                ExtractorBorsaItalianaTask('indici')
                ]

    def run(self):
        input_df = {}
        input_value = self.input()
        input_value = [input_value] if type(input_value) is LocalTarget else input_value
        for table_in in input_value:
            if isinstance(table_in, LocalTarget):
                in_file = table_in.path
                in_name = in_file.replace(self.data_dir + "/", "").replace(".csv", "")
                df = pd.read_csv(in_file, header=0, sep='\t', keep_default_na=False, na_values=[''])  # dtype=str)
              #  df = df.where(df.notnull(), None)
                df = df.replace({np.nan: None})
                input_df[in_name] = df

        # populate sub-exchange table
        subexchange_data = asyncio.run(create_subexchange(input_df['stock_csi_data'], input_df['azioni_borsa_italiana'],
                                              input_df['fondi_borsa_italiana']))
        logger.info('List of all record for SubExchange table created. Ready for merging')
        asyncio.run(self.output().load_data(data=subexchange_data, table_name=SubExchanges.tablename))

        # Populate Symbology table
        symbols_data = asyncio.run(create_symbology(input_df=input_df, source_list=self.symbology_sources.split(',')))
        logger.info('List of all record for Symbology table created. Ready for merging')
        asyncio.run(self.output().load_data(data=symbols_data, table_name=Symbology.tablename))

        logger.info('Complete SYMBOLOGY LOAD process!')
        self.task_complete = True

    def output(self):
        return SqlTarget()
