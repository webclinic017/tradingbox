from traceback import format_exc
import requests
import time
import numpy as np

from bs4 import BeautifulSoup as bs

import pandas as pd

import aiohttp

from settings.settings import logger
from etl.utils.download import download_data

import ssl
import certifi

"""
handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)
"""

categories = ['azioni', ]

base_urls = {
    'azioni': """https://www.borsaitaliana.it/borsa/azioni/listino-a-z.html""",
    'etf': """https://www.borsaitaliana.it/etf/etf/infoproviders.xlsx""",
    'etc-etn': """https://www.borsaitaliana.it/etc-etn/etc-etn/infoprovider.xlsx""",
    'fondi': """https://www.borsaitaliana.it/fondi/homepage/fulllist.xlsx""",
    'indici': ["""https://www.borsaitaliana.it/borsa/azioni/tutti-gli-indici.html""",
               """https://www.borsaitaliana.it/borsa/azioni/indici-settoriali.html"""]
}

dettaglio_urls = {
    'etf': """https://www.borsaitaliana.it/borsa/etf/dettaglio.html?isin={}&lang=it""",
    'etc-etn': """https://www.borsaitaliana.it/borsa/etc-etn/dettaglio.html?isin={}&lang=it""",
    'fondi': """https://www.borsaitaliana.it/borsa/fondi/fondi-aperti/dettaglio.html?isin={}&lang=it""",
    'azioni': """https://www.borsaitaliana.it/borsa/azioni/dati-completi.html?isin={}&lang=it"""
}

base_http = "https://www.borsaitaliana.it"

assets = ['etf', 'etc-etn', 'fondi']


########################################################################################################################
#                   COMMON FUNCTIONS
#
def get_xls_from_borsaitaliana():
    xls = pd.DataFrame()
    for asset in assets:
        try:
            url = base_urls[asset]

            resp = download_data(url, 0)

            df = pd.read_excel(resp.content, engine='openpyxl')
            df.dropna(how='all', inplace=True)
            df.dropna(axis=1, how='all', inplace=True)
            df.columns = df.iloc[0].values
            df.columns = df.columns.str.upper()
            df = df.iloc[1:]
        except Exception as e:
            logger.error('Something went wrong during downloading XLS from Borsa Italiana')
            logger.error(str(e))
            xls = pd.DataFrame()

       # df = df.where(pd.notnull(df), None)
        df = df.replace({np.nan: None})

        df = df.applymap(lambda x: str(x).strip())
        df['ASSET'] = asset
        if asset == 'fondi':
            df = df.rename(columns={'TRADING CODE': 'LOCAL MARKET TIDM', 'AREA': 'AREA BENCHMARK'})
        xls = xls.append(df)
    return xls


def get_links_from_xls(file, asset, type_id='ISIN'):
    links = []
    path = file.path
    input_df = pd.read_csv(path, sep='\t')

    input_df = input_df.loc[input_df['ASSET'] == asset]
    isin_list = input_df[type_id]

    for idx, isin in isin_list.iteritems():
        links.append(dettaglio_urls[asset].format(isin))

    return links


########################################################################################################################
#                   COMMON ASYNC FUNCTIONS
#
sslcontext = ssl.create_default_context(cafile=certifi.where())
async def get_product(link):
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url=link, ssl=sslcontext) as response:  # , params=request_params, headers=request_headers) as response:
                resp = await response.read()
                page = bs(resp, 'lxml')
                return page
        except aiohttp.ClientConnectorError as e:
            logger.error('Connection Error', str(e))
        except Exception as e:
            logger.error("Unable to get url {} due to {}.".format(link, e.__class__))
            logger.error(str(e))


########################################################################################################################
#                   AZIONI
#

def get_links_from_azioni_main_page():

    def get_page_html(params):
        baseurl = base_urls['azioni']

        resp = download_data(baseurl, 0, params)
        # resp = requests.get(baseurl, params)
        rawpage = resp.text
        page = rawpage.replace("<!-->", "")
        # self.page = bs(page, 'html.parser')
        page = bs(page, 'lxml')
        return page

    i = 1
    links_schede = []

    while i > 0:
        try:
            page = get_page_html({'page': i})
            table = page.find('table', attrs={'class': 'm-table -firstlevel'}) \
                .find_all('a', attrs={'class': 'u-hidden -xs'}, href=True)
            [links_schede.append(base_http + a['href']) for a in table]
            next = page.find('div', attrs={'class': "m-pagination"}) \
                .find_all('a', attrs={'title': 'Successiva'})
            if next:
                i += 1
            else:
                i = 0
        except Exception as e:
            logger.error('Something went wrong getting tickers from Borsa Italiana')
            logger.error(str(e))
            links_schede = []
            break

    preset = 'scheda/'
    postset = '.html?lang'
    links_data_completi = []
    for link in links_schede:
        start = link.find(preset) + len(preset)
        end = link.find(postset)
        link_completo = dettaglio_urls['azioni'].format(link[start:end])
        links_data_completi.append(link_completo)

    return links_schede, links_data_completi


async def get_azioni_from_links(link, rate_limiter):
    info = None
    if link:
        try:
            async with rate_limiter.throttle():
                dati_completi = await get_product(link)

            title = dati_completi.find('h1', attrs={'class': 't-text -flola-bold -size-xlg -inherit'}).text
            tables = dati_completi.find('div', attrs={'class': 'l-box -pb -pt | h-bg--white'}) \
                .find_all('table', attrs={'class', 'm-table -clear-m'})
            rows = []
            for table in tables:
                row = table.find_all('tr')
                rows.extend(row)
            info = {}
            for row in rows:
                if not row.find_all('a'):
                    k, v = tuple(map(lambda x: x.text, row.find_all('td')))
                    k = 'Mercato' if 'mercato/segmento' in k.lower() else k
                    key = k.replace(":", "").strip()
                    v = v.replace(":", "").strip()
                    value = None if v == "" or v == "-" else v
                    info[key] = value

            if not 'Super Sector' in info.keys():
                info['Super Sector'] = None

            info['Nome'] = title.strip()

        except Exception as e:
            logger.error('Errore acquisizione pagina ' + link)
            logger.error(str(e))

    return info


async def get_indici_from_azioni(link, rate_limiter):
    indici = None
    info = None
    symbols = ""
    if link:
        codice_isin = link.split('scheda/')[1].split('.html')[0]
        info = {'Codice Isin': codice_isin}
        try:
            async with rate_limiter.throttle():
                page = await get_product(link)
            try:
                table_indici = page.find('table', attrs={'class': 'm-table -clear-mtop -indice'}).find_all('tr')
                for row in table_indici:
                    k, v = tuple(map(lambda x: x, row.find_all('td')))
                    items = v.find_all('a')
                    if len(items) > 0:
                        for item in items:
                            a = item['href'].replace('&lang=it', '')
                            symbol = a.split('indexCode=')[1]
                            symbols += symbol + ','
                        indici = symbols[:-1]
                    else:
                        indici = None
            except:
                indici = None

        except Exception as e:
            logger.error('Errore acquisizione pagina ' + link)
            logger.error(str(e))

        info['indici'] = indici
    return info


########################################################################################################################
#                   ETF
#

async def get_etf_from_links(link, rate_limiter):
    info = None
    if link:
        try:
            async with rate_limiter.throttle():
                dati_completi = await get_product(link)

            tables = dati_completi.find('div', attrs={'class': 'l-box -pb -pt | h-bg--white'}) \
                .find_all('table', attrs={'class', 'm-table -clear-m'})
            rows = []
            for table in tables:
                row = table.find_all('tr')
                rows.extend(row)
            info = {}
            for row in rows:
                if not row.find_all('a'):
                    k, v = tuple(map(lambda x: x.text, row.find_all('td')))
                    key = k.replace('Area Benchmark', 'Area')
                    key = key.replace(":", "").strip()
                    v = v.replace(":", "").strip()
                    value = None if v == "" or v == "-" else v
                    info[key] = value

        except Exception as e:
            logger.error('Errore acquisizione pagina ' + link)
            logger.error(str(e))

    return info


########################################################################################################################
#                   ETC ETN
#
async def get_etc_etn_from_links(link, rate_limiter):
    info = None
    if link:
        try:
            async with rate_limiter.throttle():
                dati_completi = await get_product(link)

            tables = dati_completi.find('div', attrs={'class': 'l-box -pb -pt | h-bg--white'}) \
                .find_all('table', attrs={'class', 'm-table -clear-m'})
            rows = []
            for table in tables:
                row = table.find_all('tr')
                rows.extend(row)
            info = {}
            for row in rows:
                if not row.find_all('a'):
                    k, v = tuple(map(lambda x: x.text, row.find_all('td')))
                    key = k.replace(":", "").strip()
                    v = v.replace(":", "").strip()
                    value = None if v == "" or v == "-" else v
                    info[key] = value

        except Exception as e:
            logger.error('Errore acquisizione pagina ' + link)
            logger.error(str(e))

    return info


########################################################################################################################
#                   FONDI
#
async def get_fondi_from_links(link, rate_limiter):
    info = None
    if link:
        try:
            async with rate_limiter.throttle():
                dati_completi = await get_product(link)

            tables = dati_completi.find('div', attrs={'class': 'l-box -pb -pt | h-bg--white'}) \
                .find_all('table', attrs={'class', 'm-table -clear-m'})
            rows = []
            for table in tables:
                row = table.find_all('tr')
                rows.extend(row)
            info = {}
            for row in rows:
                if not row.find_all('a'):
                    k, v = tuple(map(lambda x: x.text, row.find_all('td')))
                    key = k.replace(":", "").strip()
                    v = v.replace(":", "").strip()
                    value = None if v == "" or v == "-" else v
                    info[key] = value

        except Exception as e:
            logger.error('Errore acquisizione pagina ' + link)
            logger.error(str(e))

    return info


########################################################################################################################
#                   INDICI
#
def get_links_from_indici_main_page():

    def get_page_html(link):
        try:
            resp = download_data(link, 0)
            # resp = requests.get(link)
            page = bs(resp.text, 'lxml')
            return page
        except Exception as e:
            logger.error("Unable to get url {} due to {}.".format(link, e.__class__))
            logger.error(str(e))

    links = []
    urls = base_urls['indici']
    for url in urls:
        page = get_page_html(url)
        if page:
            try:
                tables = page.find_all('table', attrs={'class': 'm-table -firstlevel'}) \
                    #                .find_all('a', attrs={'class': 'u-hidden -xs'}, href=True)
                rows = []
                for table in tables:
                    tags = table.find_all('a', attrs={'class': 'u-hidden -xs'}, href=True)
                    [links.append(base_http + a['href']) for a in tags]
            except Exception as e:
                logger.error('Something went wrong getting tickers from Borsa Italiana')
                logger.error(str(e))

    logger.info("List of INDICI links completed!")
    return links


async def get_indici_from_links(link, rate_limiter):
    info = None
    if link:

        try:
            async with rate_limiter.throttle():
                dati_completi = await get_product(link)

            title = dati_completi.find('h1', attrs={'class': 't-text -flola-bold -size-xlg -inherit'}).text
            tables = dati_completi.find('div', attrs={'class': 'l-box -pb -pt | h-bg--white'}) \
                .find_all('table', attrs={'class', 'm-table -clear-mtop'})
            codice_alfanumerico = link[link.find("=") + 1:].replace("&lang=it", "")
            rows = []
            for table in tables:
                row = table.find_all('tr')
                rows.extend(row)
            info = {}
            for row in rows:
                if not row.find_all('a'):
                    k, v = tuple(map(lambda x: x.text, row.find_all('td')))
                    key = k.replace(":", "").strip()
                    v = v.replace(":", "").strip()
                    value = None if v == "" or v == "-" else v
                    info[key] = value

            info['Nome'] = title.strip()
            info['Codice Alfanumerico'] = codice_alfanumerico

        except Exception as e:
            logger.error('Errore acquisizione pagina ' + link)
            logger.error(str(e))

    return info
