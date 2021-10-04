import os
import datetime

from etl.load_initialize import CreateDB, LoadInitializeTask
from etl.load_data_symbology import LoadDataSymbologyTask
from etl.load_fundamentals_table import LoadFundamentalsTable
from etl.load_ticker_table import LoadTickerTable, LoadIndicesTable
from etl.load_price_table import LoadPriceTable

from settings.settings import LUIGI_CONFIG_PATH, logger

import luigi
luigi.configuration.add_config_path(LUIGI_CONFIG_PATH)

#######################################################################################################################


class EquitiesDownloadTask(luigi.WrapperTask):
    # start_date = luigi.DateParameter(default=None)
    # end_date = luigi.DateParameter(default=None)
    drop = luigi.BoolParameter(default=False)

    def requires(self):
        yield CreateDB()
        yield LoadInitializeTask()
        yield LoadDataSymbologyTask()
     #   yield LoadFundamentalsTable()
      #  yield LoadTickerTable()
       # yield LoadIndicesTable()
        #yield LoadPriceTable(daily_downloads='yahoo,alpaca', minute_downloads='')


#######################################################################################################################

def clean_downloads():
    dir_path = luigi.configuration.get_config().get('table', 'PATH_DOWNLOAD_FILE', None)
    weekday = datetime.datetime.today().weekday()

    for f in os.listdir(dir_path):
        if not 'yahoo_data' in f or weekday == 5:
            try:
                os.remove(os.path.join(dir_path, f))
            except (OSError, Exception):
                continue


def run_luigi_task(mode: str, workers: int = 1, local=True, force=False):
    if force:
        clean_downloads()

    if mode == 'BACKFILL':
        task = EquitiesDownloadTask()
    elif mode == 'DAILY':
        task = EquitiesDownloadTask()
    else:
        raise ValueError(f'unknown mode: {mode}')
    try:
        luigi.build([task], workers=workers, local_scheduler=local)
    except:
        logger.ERROR("Something's wrong on ETL WORKER - Restarting Now ...")

