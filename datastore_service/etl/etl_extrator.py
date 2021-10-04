import os
import time
import asyncio
import pandas as pd
import numpy as np

from sys import platform

"""
from database.models.data import Italy_Stock, Italy_ETF, Italy_ETC_ETN, Italy_FONDI, Italy_INDICI, Italy_XLS_FILE, \
    CSI_Stock
"""
from etl.utils.download import download_csidata_factsheet
from etl.utils.ratelimiter import RateLimiter

from etl.utils.etl_bi_scraper import get_xls_from_borsaitaliana, get_links_from_azioni_main_page, \
    get_azioni_from_links, get_indici_from_azioni, get_links_from_xls, get_etf_from_links, get_etc_etn_from_links, \
    get_fondi_from_links, get_indici_from_links, get_links_from_indici_main_page

from settings.config import DATA_TYPE_STOCK
from settings.settings import logger, LUIGI_CONFIG_PATH

import luigi
luigi.configuration.add_config_path(LUIGI_CONFIG_PATH)


nothing = [None, np.nan, 'None', 'n/a', 'none', 'NONE']

"""
if platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
else:
"""
asyncio.set_event_loop(asyncio.new_event_loop())


########################################################################################################################
#                                           MAIN FUCTION


class ExtractorCSIDataTask(luigi.Task):
    data_type = luigi.Parameter(default=None)  # DATA_TYPE_STOCK)

    #  csidata_type = luigi.configuration.get_config().get('data_extractor', 'csidata_type', None)
    #  symbology_sources = luigi.configuration.get_config().get('data_extractor', 'symbology_sources', None)
    data_dir = luigi.configuration.get_config().get('table', 'PATH_DOWNLOAD_FILE', None)
    input_dir = luigi.configuration.get_config().get('table', 'PATH_CSV_TABLES', None)

    def load_csi_link(self):
        file = os.path.abspath(os.path.join(self.input_dir, 'csi_link.csv'))
        # Read file into dataframe
        df = pd.read_csv(file, sep=';', encoding='ISO-8859-1')
        if self.data_type:
            df = df.loc[df['csi_type'] == self.data_type]
        return df

    def output(self):
        return luigi.LocalTarget(f'{self.data_dir}/{self.data_type}_csi_data.csv')

    def run(self):
        # Always extract CSI values, as they are used for the symbology table
        start_time = time.time()
        with self.output().temporary_path() as path:
            logger.info(f'Downloading {self.data_type} from CSI DATA to {path}')

            csi_links = self.load_csi_link()

            # Add new CSI Data tables to this if block
            if self.data_type == DATA_TYPE_STOCK:
                try:
                    # Read file into dataframe
                    csi_data = pd.DataFrame()
                    for index, link in csi_links.iterrows():
                        if pd.isna(link['csi_exchangeid']):
                            if not link['isetn'] and not link['isetf']:
                                csi_data = download_csidata_factsheet(link['csi_link'], self.data_type)

                            elif link['isetf']:
                                etfs = download_csidata_factsheet(link['csi_link'], self.data_type)
                                csi_data['is_etf'] = np.where(csi_data['csi_number'].isin(list(etfs['csi_number'])),
                                                              True, False)

                            elif link['isetn']:
                                etns = download_csidata_factsheet(link['csi_link'], self.data_type)
                                csi_data['is_etn'] = np.where(csi_data['csi_number'].isin(list(etns['csi_number'])),
                                                              True, False)

                    # csv_data = pd.read_csv('_csv_tables/stockfactsheet.csv')  # , sep=',', encoding='ISO-8859-1')
                    # Replace NaN values
                    csi_data = csi_data.drop(['switch_cf_date', 'pre_switch_cf'], axis=1)
                    csi_data = csi_data.where(csi_data.notnull(), None)
                    # Save data as csv file
                    csi_data.to_csv(path, header=True, sep='\t')
                    logger.info(f'All records of {self.data_type} from CSI DATA are stored in {path} '
                                f'| %0.1f seconds' % (time.time() - start_time))
                except Exception as e:
                    logger.error(f'Unable to export {self.data_type} from CSI DATA.Skipping it')
                    logger.error(str(e))


class DownloadXLSBorsaItalianaTask(luigi.Task):
    # Download XLS file for ETF, ETC/ETN and FONDI from Borsa Italiana website

    data_dir = luigi.configuration.get_config().get('table', 'PATH_DOWNLOAD_FILE', None)

    def output(self):
        return luigi.LocalTarget(f'{self.data_dir}/XLS_borsa_italiana.csv')

    def run(self):
        # Download and store XLS file for ETF, ETC/ETN and FONDI from Borsa Italiana
        start_time = time.time()
        with self.output().temporary_path() as path:
            try:
                logger.info(f'Downloading XLS data for ETF, ETC/ETN and FONDI from Borsa Italiana to {path}')

                xls_borsaitaliana = get_xls_from_borsaitaliana()
                xls_borsaitaliana.to_csv(path, header=True, sep='\t')

                logger.info(f'XLS from Borsa Italiana is stored in csv file '
                            f'| %0.1f seconds' % (time.time() - start_time))
            except Exception as e:
                logger.info(
                    f"Something wrong while processing XLS BorsaItaliana Download... Skip it!")
                logger.error(str(e))


class ExtractorBorsaItalianaTask(luigi.Task):
    asset = luigi.Parameter()
    data_dir = luigi.configuration.get_config().get('table', 'PATH_DOWNLOAD_FILE', None)

    def requires(self):
        return DownloadXLSBorsaItalianaTask()  # self.start_date, self.end_date)

    def output(self):
        return luigi.LocalTarget(f'{self.data_dir}/{self.asset}_borsa_italiana.csv')

    def run(self):
        with self.output().temporary_path() as path:
            #   loop = asyncio.get_event_loop()
            try:
                if 'azioni' in self.asset:
                    logger.info(f'Scraping AZIONI from Borsa Italiana to {path}')
               #     loop = asyncio.get_event_loop()
                    dati = asyncio.run(self.get_azioni())

                elif 'etf' in self.asset:
                    logger.info(f'Scraping ETF from Borsa Italiana to {path}')
                 #   loop = asyncio.get_event_loop()
                    dati = asyncio.run(self.get_etf())
                elif 'etc-etn' in self.asset:
                    logger.info(f'Scraping ETC-ETN from Borsa Italiana to {path}')
                    #loop = asyncio.get_event_loop()
                    dati = asyncio.run(self.get_etc_etn())
                elif 'fondi' in self.asset:
                    logger.info(f'Scraping FONDI from Borsa Italiana to {path}')
                    #loop = asyncio.get_event_loop()
                    dati = asyncio.run(self.get_fondi())
                elif 'indici' in self.asset:
                    logger.info(f'Scraping INDICI from Borsa Italiana to {path}')
                    #loop = asyncio.get_event_loop()
                    dati = asyncio.run(self.get_indici())

                dati.to_csv(path, header=True, sep='\t')
            except Exception as e:
                logger.info(f"Something wrong while processing BorsaItaliana Extractor for "
                            f"{str(self.asset).upper()}... Skip it!")
                logger.error(str(e))

    async def get_azioni(self):
        links_schede_azioni, links_dati_completi = get_links_from_azioni_main_page()

        logger.info("Now analysing all Dati Completi AZIONI links")
        async with RateLimiter(rate_limit=int(500 / 60), concurrency_limit=10) as rate_limiter:
            azioni_dati_completi = await asyncio.gather(
                *[get_azioni_from_links(link, rate_limiter) for link in links_dati_completi])

        logger.info("Now analysing all Indici AZIONI links")
        async with RateLimiter(rate_limit=int(500 / 60), concurrency_limit=10) as rate_limiter:
            azioni_indici = await asyncio.gather(
                *[get_indici_from_azioni(link, rate_limiter) for link in links_schede_azioni])

        dati_completi = {}
        for idx, azioni in enumerate(azioni_dati_completi):
            dati_completi[azioni['Codice Isin']] = azioni

        data = pd.DataFrame()
        for idx, indice in enumerate(azioni_indici):
            try:
                dati_completi[indice['Codice Isin']]['Indici'] = indice['indici']
                data = data.append(dati_completi[indice['Codice Isin']], ignore_index=True)
            except Exception as e:
                logger.error('Errore acquisizione dati completi azioni: ' + indice['Codice Isin'])
                logger.error(str(e))

        # logger.info('Borsa Italiana for Azione Extractor done in %0.1f seconds' % (time.time() - start_time))
    #    data = data.where(data.notnull(), None)
        data = data.replace({np.nan: None})
        data.rename(columns=azioni_columns(), inplace=True)
        return data

    async def get_etf(self):
        etf_links = get_links_from_xls(self.input(), 'etf')

        logger.info("Now analysing all ETF links")
        async with RateLimiter(rate_limit=int(500 / 60), concurrency_limit=10) as rate_limiter:
            etf_dati = await asyncio.gather(*[get_etf_from_links(link, rate_limiter) for link in etf_links])

        etf_dati_completi = list(filter(None, etf_dati))
        data = pd.DataFrame.from_dict(etf_dati_completi)
        #    data = data.where(data.notnull(), None)
        data = data.replace({np.nan: None})
        data.rename(columns=etf_columns(), inplace=True)
        # logger.info('Borsa Italiana for ETF Extractor done in %0.1f seconds' % (time.time() - start_time))
        return data

    async def get_etc_etn(self):
        etc_etn_links = get_links_from_xls(self.input(), 'etc-etn')

        logger.info("Now analysing all ETC ETN links")
        async with RateLimiter(rate_limit=int(500 / 60), concurrency_limit=10) as rate_limiter:
            etc_etn_dati = await asyncio.gather(*[get_etc_etn_from_links(link, rate_limiter) for link in etc_etn_links])

        etc_etn_dati_completi = list(filter(None, etc_etn_dati))
        data = pd.DataFrame.from_dict(etc_etn_dati_completi)
        #    data = data.where(data.notnull(), None)
        data = data.replace({np.nan: None})

        data.rename(columns=etc_etn_columns(), inplace=True)
        # logger.info('Borsa Italiana for ETC ETN Extractor done in %0.1f seconds' % (time.time() - start_time))
        return data

    async def get_fondi(self):
        fondi_links = get_links_from_xls(self.input(), 'fondi')

        logger.info("Now analysing all FONDI links")
        async with RateLimiter(rate_limit=int(500 / 60), concurrency_limit=10) as rate_limiter:
            fondi_dati = await asyncio.gather(*[get_fondi_from_links(link, rate_limiter) for link in fondi_links])

        fondi_dati_completi = list(filter(None, fondi_dati))
        data = pd.DataFrame.from_dict(fondi_dati_completi)
        #    data = data.where(data.notnull(), None)
        data = data.replace({np.nan: None})
        data.rename(columns=fondi_columns(), inplace=True)
        # logger.info('Borsa Italiana for FONDI Extractor done in %0.1f seconds' % (time.time() - start_time))
        return data

    async def get_indici(self):
        indici_links = get_links_from_indici_main_page()

        logger.info("Now analysing all INDICI links")
        async with RateLimiter(rate_limit=int(500 / 60), concurrency_limit=10) as rate_limiter:
            indici_dati = await asyncio.gather(*[get_indici_from_links(link, rate_limiter) for link in indici_links])

        indici_dati_completi = list(filter(None, indici_dati))
        data = pd.DataFrame.from_dict(indici_dati_completi)
        #    data = data.where(data.notnull(), None)
        data = data.replace({np.nan: None})
        data.rename(columns=indici_columns(), inplace=True)
        data['codice_isin'] = data['codice_alfanumerico'].copy()
        # logger.info('Borsa Italiana for INDICI Extractor done in %0.1f seconds' % (time.time() - start_time))
        return data


######################################################################################################################

def azioni_columns():
    return {
        'Codice Isin': 'codice_isin',
        'ID Strumento': 'id_strumento',
        'Codice Alfanumerico': 'codice_alfanumerico',
        'Nome': 'nome',
        'Mercato': 'mercato',
        'Cap Sociale': 'cap_sociale',
        'Capitalizzazione': 'capitalizzazione',
        'Lotto Minimo': 'lotto_minimo',
        'Fase di Mercato': 'fase_mercato',
        'Prezzo Ultimo Contratto': 'prezzo_ultimo_contratto',
        'Var %': 'var_per',
        'Var Assoluta': 'var_assoluta',
        'Pr Medio Progr': 'pr_medio_progr',
        'Data - Ora Ultimo Contratto': 'data_ora_ultimo_contratto',
        'Quantità Ultimo': 'quantita_ultimo',
        'Quantità Acquisto': 'quantita_acquisto',
        'Prezzo Acquisto': 'prezzo_acquisto',
        'Prezzo Vendita': 'prezzo_vendita',
        'Quantità Vendita': 'quantita_vendita',
        'Quantità Totale': 'quantita_totale',
        'Numero Contratti': 'numero_contratti',
        'Controvalore': 'controvalore',
        'Max Oggi': 'max_oggi',
        'Max Anno': 'max_anno',
        'Min Oggi': 'min_oggi',
        'Min Anno': 'min_anno',
        'Chiusura': 'chiusura',
        'Prezzo di riferimento': 'prezzo_riferimento',
        'Prezzo ufficiale': 'prezzo_ufficiale',
        'Pre-Apertura': 'pre_apertura',
        'Performance 1 mese': 'performance_1_mese',
        'Performance 6 mesi': 'performance_6_mesi',
        'Performance 1 anno': 'performance_1_anno',
        'Super Sector': 'super_sector',
        'Indici': 'indici',
        'Apertura Odierna': 'apertura_odierna',
        'Prezzo di Esercizio': 'prezzo_esercizio',
        'Rapporto di Esercizio': 'rapporto_esercizio'
    }


def etf_columns():
    return {
        'Denominazione': 'nome',
        'Tipo strumento': 'tipo_strumento',
        'Segmento': 'segmento',
        'Classe': 'classe',
        'Codice Isin': 'codice_isin',
        'Codice Alfanumerico': 'codice_alfanumerico',
        'Fase di Mercato': 'fase_mercato',
        'Apertura': 'apertura',
        'Var %': 'var_per',
        'Var Assoluta': 'var_assoluta',
        'Pr Medio Progr': 'pr_medio_progr',
        'Data - Ora Ultimo Contratto': 'data_ora_ultimo_contratto',
        'Volume Ultimo': 'volume_ultimo',
        'Volume Acquisto': 'volume_acquisto',
        'Prezzo Acquisto': 'prezzo_acquisto',
        'Prezzo Vendita': 'prezzo_vendita',
        'Volume Vendita': 'volume_vendita',
        'Lotto Minimo': 'lotto_minimo',
        'Volume totale': 'volume_totale',
        'Numero Contratti': 'numero_contratti',
        'Controvalore': 'controvalore',
        'EMS': 'ems',
        'Obblighi di quotazione (Max spread)': 'obblighi_quotazione',
        'Performance 1 mese': 'performance_1_mese',
        'Performance 6 mesi': 'performance_6_mesi',
        'Performance da inizio anno': 'performance_inizio_anno',
        'Performance 1 anno': 'performance_1_anno',
        'Max oggi': 'max_oggi',
        'Max Anno': 'max_anno',
        'Data max Anno': 'data_max_anno',
        'Min oggi': 'min_oggi',
        'Min Anno': 'min_anno',
        'Data min Anno': 'data_min_anno',
        'Prezzo asta di chiusura odierna': 'prezzo_asta_chiusura_odierna',
        'Prezzo di riferimento': 'prezzo_riferimento',
        'Prezzo ufficiale': 'prezzo_ufficiale',
        'Benchmark': 'benchmark',
        'Area Benchmark': 'area_benchmark',
        'Emittente': 'emittente',
        'Commissioni totali annue': 'commissioni_totali_annue',
        'Commissioni entrata uscita Performance': 'commissioni_entrata_uscita_performance',
        'Valuta di Denominazione': 'valuta_denominazione',
        'Dividendi': 'dividendi',
        'Reuters Ric Strumento': 'reuters_ric_strumento',
        'Bloomberg Ticker Strumento': 'bloomberg_ticker_strumento',
        'iNAV - Reuters Ric': 'inav_reuters_ric',
        'iNAV - Bloomberg Ticker': 'inav_bloomberg_ticker',
        'Livello di protezione': 'livello_protezione',
        'Multiplier': 'multiplier'
    }


def etc_etn_columns():
    return {
        'Denominazione': 'nome',
        'Tipo strumento': 'tipo_strumento',
        'Segmento': 'segmento',
        'Classe': 'classe',
        'Codice Isin': 'codice_isin',
        'Codice Alfanumerico': 'codice_alfanumerico',
        'Fase di Mercato': 'fase_mercato',
        'Apertura': 'apertura',
        'Var %': 'var_per',
        'Var Assoluta': 'var_assoluta',
        'Pr Medio Progr': 'pr_medio_progr',
        'Data - Ora Ultimo Contratto': 'data_ora_ultimo_contratto',
        'Volume Ultimo': 'volume_ultimo',
        'Volume Acquisto': 'volume_acquisto',
        'Prezzo Acquisto': 'prezzo_acquisto',
        'Prezzo Vendita': 'prezzo_vendita',
        'Volume Vendita': 'volume_vendita',
        'Lotto Minimo': 'lotto_minimo',
        'Volume totale': 'volume_totale',
        'Numero Contratti': 'numero_contratti',
        'Controvalore': 'controvalore',
        'EMS': 'ems',
        'Obblighi di quotazione (Max spread)': 'obblighi_quotazione',
        'Performance 1 mese': 'performance_1_mese',
        'Performance 6 mesi': 'performance_6_mesi',
        'Performance da inizio anno': 'performance_inizio_anno',
        'Performance 1 anno': 'performance_1_anno',
        'Max oggi': 'max_oggi',
        'Max Anno': 'max_anno',
        'Data max Anno': 'data_max_anno',
        'Min oggi': 'min_oggi',
        'Min Anno': 'min_anno',
        'Data min Anno': 'data_min_anno',
        'Prezzo asta di chiusura odierna': 'prezzo_asta_chiusura_odierna',
        'Prezzo di riferimento': 'prezzo_riferimento',
        'Prezzo ufficiale': 'prezzo_ufficiale',
        'Sottostante': 'sottostante',
        'Tipo Sottostante': 'tipo_sottostante',
        'Emittente': 'emittente',
        'Commissioni totali annue': 'commissioni_totali_annue',
        'Commissioni entrata uscita Performance': 'commissioni_entrata_uscita_performance',
        'Valuta di Denominazione': 'valuta_denominazione',
        'Dividendi': 'dividendi',
        'Reuters Ric Strumento': 'reuters_ric_strumento',
        'Bloomberg Ticker Strumento': 'bloomberg_ticker_strumento'
    }


def fondi_columns():
    return {
        'Denominazione': 'nome',
        'Tipo strumento': 'tipo_strumento',
        'Mercato': 'mercato',
        'Segmento': 'segmento',
        'Codice Isin': 'codice_isin',
        'Codice Alfanumerico': 'codice_alfanumerico',
        'Fase di Mercato': 'fase_mercato',
        'Ultimo NAV': 'ultimo_NAV',
        'Data Ultimo NAV': 'data_ultimo_NAV',
        'Volume Acquisto': 'volume_acquisto',
        'Prezzo Acquisto': 'prezzo_acquisto',
        'Prezzo Vendita': 'prezzo_vendita',
        'Volume Vendita': 'volume_vendita',
        'Lotto Minimo': 'lotto_minimo',
        'Volume totale': 'volume_totale',
        'Numero Contratti': 'numero_contratti',
        'EMS': 'ems',
        'Performance 1 mese': 'performance_1_mese',
        'Performance 6 mesi': 'performance_6_mesi',
        'Performance da inizio anno': 'performance_inizio_anno',
        'Performance 1 anno': 'performance_1_anno',
        'Max Anno': 'max_anno',
        'Data max Anno': 'data_max_anno',
        'Min Anno': 'min_anno',
        'Data min Anno': 'data_min_anno',
        'Prezzo di riferimento': 'prezzo_riferimento',
        'Area': 'area_benchmark',
        'Emittente': 'emittente',
        'Valuta di Denominazione': 'valuta_denominazione',
        'Dividendi': 'dividendi',
        'Reuters Ric Strumento': 'reuters_ric_strumento',
        'Bloomberg Ticker Strumento': 'bloomberg_ticker_strumento'
    }


def indici_columns():
    return {
        'Codice Alfanumerico': 'codice_alfanumerico',
        'Nome': 'nome',
        'Max Oggi': 'max_oggi',
        'Min Oggi': 'min_oggi',
        'Chiusura precedente': 'chiusura_precedente',
        'Max Anno - Data': 'max_anno_data',
        'Min Anno - Data': 'min_anno_data',
        'Max Anno Prima': 'max_anno_prima',
        'Min Anno Prima': 'min_anno_prima',
        'Chiusura Anno Prima': 'chiusura_anno_prima',
        'Primo Valore': 'primo_valore',
        'Performance 1 mese': 'performance_1_mese',
        'Performance 6 mesi': 'performance_6_mesi',
        'Performance 1 anno': 'performance_1_anno',
        'Performance 2 Anni': 'performance_2_Anni',
        'TR Ultima Chiusura': 'TR_ultima_chiusura',
        'TR Var % ': 'TR_var_per',
        'TR Data Chiusura': 'TR_data_chiusura'
    }
