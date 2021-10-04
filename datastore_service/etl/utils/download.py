from datetime import datetime
import numpy as np
import pandas as pd
import time
import os
import io
import requests
from requests.exceptions import ConnectionError

import aiohttp

# from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from traceback import format_exc

from settings.settings import logger  # DOWNLOAD_LIST,
from settings.config import CSI_STOCK_COLUMN_NAMES, DATA_TYPE_STOCK

'''
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0",
}


def load_csv_file(filename, path=''):
    csv_file = os.path.abspath(os.path.join(path, filename))
    df = pd.DataFrame()
    try:
        # df = pd.read_csv(csv_file, encoding='utf-8', low_memory=False)
        df = pd.read_csv(csv_file, encoding='ISO-8859-1')  # encoding='utf-8')

    except Exception as e:
        logger.info('Error to loadn CSV file from %s' % csv_file)
        logger.error(format_exc(e))
    finally:
        return df


def download_CSI_link(path=''):
    # Read file into dataframe
    file = os.path.abspath(os.path.join(path, 'csi_link.csv'))
    # Read file into dataframe
    csv_data = pd.read_csv(file, sep=';', encoding='ISO-8859-1')

    for index, row in csv_data.iterrows():
        name = row['csi_name']
        link = row['csi_link']
        df = pd.read_csv(link, encoding='ISO-8859-1')
        df.to_csv('output/' + name + '.csv')

        print("")


def download_csidata_factsheet(url_string, data_type, exchange_id=None):
    """ Downloads the CSV factsheet for the provided data_type (stocks,
    commodities, currencies, etc.). A DataFrame is returned.

    http://www.csidata.com/factsheets.php?type=stock&format=csv

    :param url_string: String of the url root for the CSI Data website
    :param data_type: String of the data to download
    :param exchange_id: None or integer of the specific exchange to download
    :return:
    """

    #  url_string = db_url + 'type=' + data_type + '&format=' + data_format

    if exchange_id:
        url_string += '&exchangeid=' + exchange_id

    download_try = 0

    resp = download_data(url_string, download_try)
    s = resp.content
    csv_file = io.StringIO(s.decode('iso-8859-1'))
    try:
        # df = pd.read_csv(csv_file, encoding='iso-8859-1', low_memory=False, na_filter=False)
        df = pd.read_csv(csv_file, na_filter=False)
        df = df.replace('', np.NaN)
        # Rename column headers to a standardized format
        if data_type == DATA_TYPE_STOCK:
            df.columns = CSI_STOCK_COLUMN_NAMES

            df['start_date'] = df.apply(datetime_to_iso, axis=1, args=('start_date',))
            df['end_date'] = df.apply(datetime_to_iso, axis=1, args=('end_date',))
            df['switch_cf_date'] = df.apply(datetime_to_iso, axis=1, args=('switch_cf_date',))

    except Exception as e:
        logger.info('Error occurred when processing CSI %s data in '
                    'download_csidata_factsheet' % data_type)
        logger.error(format_exc(e))
        return pd.DataFrame()

    df.insert(len(df.columns), 'created_date', datetime.now().isoformat())
    df.insert(len(df.columns), 'updated_date', datetime.now().isoformat())

    return df


TIME_SLEEP = 2


def download_data(url, download_try, params=None):
    """ Downloads the data from the url provided.

    :param url: String that contains the url of the data to download.
    :param download_try: Integer of the number of attempts to download data.
    :param params: request optional parameters
    :return: A CSV file as a url object
    """

    download_try += 1
    # Download the data
    try:
        #   file = urlopen(url)
        file = requests.get(url, params)
        return file
    except ConnectionError as e:
        if download_try <= 5:
            logger.error('ConnectionError:  Problems with Connection Network. Program will sleep for '
                         '1 minutes and will try again...')
            time.sleep(TIME_SLEEP)
            return download_data(url, download_try, params)
        else:
            logger.error('ConnectionError:  Problems with Connection Network. After trying 5 time, the download '
                         'was still not successful. You could check the internet connection.')
            raise OSError(str(e))
    except HTTPError as e:
        if 'http error 403' in str(e).lower():
            # HTTP Error 403: Forbidden
            raise OSError('HTTPError %s: Reached API call limit. Make the '
                          'RateLimit more restrictive.' % (e.reason,))
        elif 'http error 404' in str(e).lower():
            # HTTP Error 404: Not Found
            raise OSError('HTTPError %s: not found' % (e.reason,))
        elif 'http error 429' in str(e).lower():
            # HTTP Error 429: Too Many Requests
            if download_try <= 5:
                logger.info('HTTPError %s: Exceeded API limit. Make the '
                            'RateLimit more restrictive. Program will sleep for '
                            '11 minutes and will try again...' % (e.reason,))
                time.sleep(11 * 60)
                return download_data(url, download_try, params)
            else:
                msg = 'HTTPError %s: Exceeded API limit. After trying 5 time, the download was still not successful. ' \
                      'You could have hit the per day call limit.' % (e.reason,)
                logger.error(msg)
                raise OSError()
        elif 'http error 500' in str(e).lower():
            # HTTP Error 500: Internal Server Error
            if download_try <= 10:
                logger.info('HTTPError %s: Internal Server Error' % (e.reason,))
        elif 'http error 502' in str(e).lower():
            # HTTP Error 502: Bad Gateway
            if download_try <= 10:
                logger.info('HTTPError %s: Encountered a bad gateway with the '
                            'server. Maybe the network is down. Will sleep for '
                            '5 minutes'
                            % (e.reason,))
                time.sleep(5 * 60)
                return download_data(url, download_try, params)
            else:
                msg = 'HTTPError %s: Server is currently unavailable. After trying 10 times, the download was still ' \
                      'not successful. Quitting for now.' % (e.reason,)
                logger.error(msg)
                raise OSError(msg)

        elif 'http error 503' in str(e).lower():
            # HTTP Error 503: Service Unavailable
            # Received this HTTP Error after 2000 queries. Browser showed
            #   captch message upon loading url.
            if download_try <= 10:
                logger.info('HTTPError %s: Server is currently unavailable. '
                            'Maybe the network is down or the server is blocking '
                            'you. Will sleep for 5 minutes...' % (e.reason,))
                time.sleep(5 * 60)
                return download_data(url, download_try, params)
            else:
                msg = 'HTTPError %s: Server is currently unavailable. After trying 10 time, the download was still ' \
                      'not successful. Quitting for now.' % (e.reason,)
                logger.error(msg)
                raise OSError(msg)
        elif 'http error 504' in str(e).lower():
            # HTTP Error 504: GATEWAY_TIMEOUT
            if download_try <= 10:
                logger.info('HTTPError %s: Server connection timed out. Maybe '
                            'the network is down. Will sleep for 5 minutes'
                            % (e.reason,))
                time.sleep(5 * 60)
                return download_data(url, download_try, params)
            else:
                msg = 'HTTPError %s: Server is currently unavailable. After trying 10 time, the ' \
                      'download was still not successful. Quitting for now.' % (e.reason,)
                logger.error(msg)
                raise OSError(msg)
        else:
            logger.info('Base URL used: %s' % (url,))
            msg = '%s - Unknown error when downloading %s' % (e, url)
            logger.error(msg)
            raise OSError(msg)

    except URLError as e:
        if download_try <= 10:
            logger.info('Warning: Experienced URL Error %s. Program will '
                        'sleep for 5 minutes and will then try again...' %
                        (e.reason,))
            time.sleep(5 * 60)
            return download_data(url, download_try, params)
        else:
            msg = 'Warning: Still experiencing URL Error %s. After trying 10 times, the error remains. ' \
                  'Quitting for now, but you can try again later.' % (e.reason,)
            logger.error(msg)
            raise URLError(msg)

    except Exception as e:
        msg = 'Warning: Encountered an unknown error when downloading %s in download_data in download.py' % (e,)
        logger.info(msg)
        logger.error(msg)
        raise OSError(msg)


def datetime_to_iso(row, column):
    """
    Change the default date format of "YYYY-MM-DD" to an ISO 8601 format
    """
    raw_date = row[column]
    try:
        raw_date_obj = datetime.strptime(raw_date, '%Y-%m-%d').isoformat()
    except TypeError:  # Occurs if there is no date provided ("nan")
        raw_date_obj = None
    return raw_date_obj


def date_to_iso(row, column):
    """
    Change the default date format of "YYYY-MM-DD" to an ISO 8601 format.
    """

    raw_date = row[column]
    try:
        raw_date_obj = datetime.strptime(raw_date, '%Y-%m-%d')
    except TypeError:  # Occurs if there is no date provided ("nan")
        raw_date_obj = datetime.today()
    return raw_date_obj.isoformat()


"""
def csv_load_converter(input):
    try:
        return float(input)
    except ValueError:
        return -1
"""