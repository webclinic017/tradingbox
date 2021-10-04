from traceback import format_exc

import requests
from bs4 import BeautifulSoup as bs

import pandas as pd

import aiohttp

from settings.settings import logger

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



########################################################################################################################
#                   COMMON ASYNC FUNCTIONS
#

def get_links_from_xls(category, id='ISIN'):
    links = []
    logger.info(
        "Now retrieving list of links: {}".format(category))
    try:
        df = pd.read_excel(base_urls[category])
        df.dropna(how='all', inplace=True)
        df.dropna(axis=1, how='all', inplace=True)
        df.columns = df.iloc[0].values
        df = df.iloc[1:]

        isin_list = df[id]

        for idx, isin in isin_list.iteritems():
            links.append(dettaglio_urls[category].format(isin))

    except Exception as e:
        logger.error('Something went wrong getting ETC ETN tickers from Borsa Italiana')
        logger.error(str(e))

    logger.info("List of {} links completed!".format(category))
    return links, df


async def get_product(link):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url=link) as response:  # , params=request_params, headers=request_headers) as response:
                resp = await response.read()
                page = bs(resp, 'lxml')
                return page
    except Exception as e:
        logger.error("Unable to get url {} due to {}.".format(link, e.__class__))
        logger.error(str(e))



########################################################################################################################
#                   AZIONI
#

def get_links_from_azioni_main_page():

    def get_page_html(params):
        baseurl=base_urls['azioni']
        response = requests.get(baseurl, params)
        rawpage = response.text
        page = rawpage.replace("<!-->", "")
        #self.page = bs(page, 'html.parser')
        page = bs(page, 'lxml')
        return page

    i = 1
    links_schede = []
    logger.info(
        "Now retrieving list of ISIN: {}".format('azioni'))
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
            continue

    preset = 'scheda/'
    postset = '.html?lang'
    links_data_completi = []
    for link in links_schede:
        start = link.find(preset)+len(preset)
        end = link.find(postset)
        link_completo = dettaglio_urls['azioni'].format(link[start:end])
        links_data_completi.append(link_completo)

    logger.info("List of {} ISIN completed!".format('azioni'))
    return links_schede, links_data_completi


async def get_azioni_from_links(link, rate_limiter):
    info = None
    if link:
        logger.info(
            "Now retrieving dati completi for links - {}".format(link))
        try:
            async with rate_limiter.throttle():
                dati_completi = await get_product(link)

            title = dati_completi.find('h1', attrs={'class': 't-text -flola-bold -size-xlg -inherit'}).text
            tables = dati_completi.find('div', attrs={'class': 'l-box -pb -pt | h-bg--white'})\
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
                    key = k.replace(":","").strip()
                    v = v.replace(":", "").strip()
                    value = None if v == "" or v == "-" else v
                    info[key] = value

            if not 'Super Sector' in info.keys():
                info['Super Sector'] = None

            info['Nome'] = title.strip()


            logger.info("Acquisiti Dati Completi per Symbol: {} - {}".format(info['Codice Alfanumerico'], info['Nome']))
        except Exception as e:
            logger.error('Errore acquisizione pagina ' + link)
            logger.error(str(e))

    return info


async def get_indici_from_azioni(link, rate_limiter):
    indici = None
    symbols = ""
    if link:
        logger.info(
            "Now retrieving info for links - {}".format(link))
        codice_isin = link.split('scheda/')[1].split('.html')[0]
        info = {'Codice Isin':codice_isin}
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
                            a = item['href'].replace('&lang=it','')
                            symbol = a.split('indexCode=')[1]
                            symbols += symbol+','
                        indici = symbols[:-1]
                    else:
                        indici = None
            except:
                indici=None

            logger.info("Acquisito Indice from: "+link)
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
       # logger.info("Now analysing all ETF links")

        logger.info(
            "Now retrieving info for links - {}".format(link))
        try:
            async with rate_limiter.throttle():
                dati_completi = await get_product(link)

        #    title = dati_completi.find('h1', attrs={'class': 't-text -flola-bold -size-xlg -inherit'}).text
            tables = dati_completi.find('div', attrs={'class': 'l-box -pb -pt | h-bg--white'})\
                                 .find_all('table', attrs={'class', 'm-table -clear-m'})
            rows = []
            for table in tables:
                row = table.find_all('tr')
                rows.extend(row)
            info = {}
            for row in rows:
                if not row.find_all('a'):
                    k, v = tuple(map(lambda x: x.text, row.find_all('td')))
                    key = k.replace(":","").strip()
                    v = v.replace(":", "").strip()
                    value = None if v == "" or v == "-" else v
                    info[key] = value

            logger.info("Acquisito Symbol: "+info['Codice Alfanumerico'])
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
#        logger.info("Now analysing all ETC ETN links")
        logger.info(
            "Now retrieving info for links - {}".format(link))
        try:
            async with rate_limiter.throttle():
                dati_completi = await get_product(link)

            #    title = dati_completi.find('h1', attrs={'class': 't-text -flola-bold -size-xlg -inherit'}).text
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

            logger.info("Acquisito ETC ETN Symbol: " + info['Codice Alfanumerico'])
        except Exception as e:
            logger.error('Errore acquisizione pagina ' + link)
            logger.error(str(e))

        return info


########################################################################################################################
#                   FONDI
#

async def get_fondi_from_links(link, rate_limiter):
    if link:
        logger.info(
            "Now retrieving info for links - {}".format(link))
        try:
            async with rate_limiter.throttle():
                dati_completi = await get_product(link)
        #    dati_completi = await asyncio.gather(*get_product(link))
            #    title = dati_completi.find('h1', attrs={'class': 't-text -flola-bold -size-xlg -inherit'}).text
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

            logger.info("Acquisito FONDI Symbol: " + info['Codice Alfanumerico'])
            return info
        except Exception as e:
            logger.error('Errore acquisizione pagina ' + link)
            logger.error(str(e))



########################################################################################################################
#                   INDICI
#
def get_links_from_indici_main_page():

    def get_page_html(link):
        try:
            response = requests.get(link)
            page = bs(response.text,'lxml')
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
        logger.info(
            "Now retrieving dati completi for links - {}".format(link))
        try:
            async with rate_limiter.throttle():
                dati_completi = await get_product(link)

            title = dati_completi.find('h1', attrs={'class': 't-text -flola-bold -size-xlg -inherit'}).text
            tables = dati_completi.find('div', attrs={'class': 'l-box -pb -pt | h-bg--white'})\
                                 .find_all('table', attrs={'class', 'm-table -clear-mtop'})
            codice_alfanumerico = link[link.find("=")+1:].replace("&lang=it","")
            rows = []
            for table in tables:
                row = table.find_all('tr')
                rows.extend(row)
            info = {}
            for row in rows:
                if not row.find_all('a'):
                    k, v = tuple(map(lambda x: x.text, row.find_all('td')))
                    key = k.replace(":","").strip()
                    v = v.replace(":", "").strip()
                    value = None if v == "" or v == "-" else v
                    info[key] = value

            info['Nome'] = title.strip()
            info['Codice Alfanumerico'] = codice_alfanumerico

            logger.info("Acquisiti Dati Completi per Symbol: "+info['Codice Alfanumerico'])
        except Exception as e:
            logger.error('Errore acquisizione pagina ' + link)
            logger.error(str(e))

    return info