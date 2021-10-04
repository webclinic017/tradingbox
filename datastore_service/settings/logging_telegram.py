import requests
import os
from pathlib import Path
from logging import Handler, Formatter
import logging
import datetime

from dotenv import load_dotenv

#import quandl

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)


TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN', None)  # 'PUT HERE YOUR TOKENID'
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', None)  # 'PUT HERE YOUR CHATID'
TELEGRAM_LOG_LEVEL = os.environ.get('TELEGRAM_LOG_LEVEL', logging.ERROR)  # 'PUT HERE YOUR CHATID'

class RequestsHandler(Handler):
    def emit(self, record):
        log_entry = self.format(record)
        payload = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': log_entry,
            'parse_mode': 'HTML'
        }
        return requests.post("https://api.telegram.org/bot{token}/sendMessage".format(token=TELEGRAM_TOKEN),
                             data=payload).content

class LogstashFormatter(Formatter):
    def __init__(self):
        super(LogstashFormatter, self).__init__()

    def format(self, record):
        t = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        return "<i>{datetime}</i>\nERROR:<pre>\n{message}</pre>".format(message=record.msg, datetime=t)