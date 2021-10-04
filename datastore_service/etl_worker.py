import time
from etl.main import run_luigi_task
from settings.config import FORCE_DELETE_ETL, WORKER_SLEEP_TIME
# import fire


if __name__ == '__main__':
    # fire.Fire(run_luigi_task('BACKFILL'))
    # FORCE_DELETE_ETL = False

    while True:
        result = run_luigi_task(mode='BACKFILL', local=False, force=FORCE_DELETE_ETL)
        if result:
            print('Sleeping for {}'.format(WORKER_SLEEP_TIME))
            time.sleep(WORKER_SLEEP_TIME)
