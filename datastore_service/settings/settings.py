import os, sys
import logging
from logging.handlers import TimedRotatingFileHandler

from pathlib import Path  # Python 3.6+ only
from dotenv import load_dotenv

from settings import logging_telegram
#import quandl

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

#env_path = Path('.') / '.env'
#load_dotenv(dotenv_path=env_path)

APP_ENV = os.environ.get('APP_ENV')
HOME = os.environ.get('HOME', '.')

DATABASE_NAME = os.environ.get('DB_NAME', None)
DATABASE_USER = os.environ.get('DB_USER', None)
DATABASE_PASSWORD = os.environ.get('DB_PASSWORD', None)

DATABASE_HOST = os.environ.get('DOCKER_TIMESCALE_HOST', None)
DATABASE_PORT = os.environ.get('DOCKER_TIMESCALE_PORT', None)

LUIGI_CONFIG_PATH = os.getenv("LUIGI_CONFIG_PATH", None)

BROKER ={
            'alpaca_paper': {
                            'BASE_URL' : 'https://paper-api.alpaca.markets',
                            'API_KEY' : os.environ.get('ALPACA_KEY_ID', None),
                            'API_SECRET': os.environ.get('ALPACA_SECRET_KEY', None),
                            }
        }

ALPACA_PAPER = BROKER['alpaca_paper']

ALPHA_VANTAGE_KEY = os.environ.get('AV_KEY', None)

QUANDL_API = os.environ.get('QUANDL_API', None)
# quandl.ApiConfig.api_key = QUANDL_API


def setup_logging(interface: str = None, level: int = logging.INFO) -> logging.Logger:
    if interface is None:
        log = logging.getLogger()
    else:
        log = logging.getLogger(interface)

    log.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(module)s - %(funcName)s - %(levelname)s - %(message)s')

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    stdout_handler.setLevel(level)
    log.addHandler(stdout_handler)
   # log.setLevel(level)

    if APP_ENV != 'test':
        timed_filehandler = TimedRotatingFileHandler('%s/worker.log' % HOME, when='D', interval=14)
        timed_filehandler.setFormatter(formatter)
        timed_filehandler.setLevel(level)
        log.addHandler(timed_filehandler)
        log.setLevel(level)

    #  TELEGRAM LOGGER

    tg_handler = logging_telegram.RequestsHandler()
    tg_formatter = logging_telegram.LogstashFormatter()
    tg_handler.setFormatter(tg_formatter)
    tg_handler.setLevel(logging_telegram.TELEGRAM_LOG_LEVEL)

    log.addHandler(tg_handler)
  #  log.setLevel(level)

    #  DATABASE LOGGER

    db_stdout_handler = logging.StreamHandler(sys.stdout)
    db_stdout_handler.setFormatter(formatter)
    db_stdout_handler.setLevel(logging.ERROR)
    log.getChild('sqlalchemy').addHandler(db_stdout_handler)
    log.getChild('sqlalchemy').setLevel(logging.ERROR)

    return log


logger = setup_logging(level=logging.INFO)

#luigi_logger = "setup_logging(interface='luigi-interface')"


