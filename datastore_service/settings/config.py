import yaml

#Read config.yml file
ymlfile = open("settings/config.yml", 'r')
config_object = yaml.load(ymlfile, Loader=yaml.FullLoader)

DOWNLOAD_LIST = config_object["download_price"]

#[database]
DATA_TYPE_STOCK = config_object["database"]["DATA_TYPE_STOCK"]
DATA_TYPE_COMODITY = config_object["database"]["DATA_TYPE_COMODITY"]
DATA_TYPE_CASH = config_object["database"]["DATA_TYPE_CASH"]

CSI_STOCK_COLUMN_NAMES = config_object["database"]["CSI_STOCK_COLUMN_NAMES"].split(',')

CSI_TSID_EXCHANGE_LIST = config_object["database"]["CSI_TSID_EXCHANGE_LIST"].split(',')
CSI_TSID_SUB_EXCHANGE_LIST = config_object["database"]["CSI_TSID_SUB_EXCHANGE_LIST"].split(',')

CSI_INDEX_EXCHANGE = config_object["database"]["CSI_INDEX_EXCHANGE"].split(',')
CSI_MUTUAL_EXCHANGE = config_object["database"]["CSI_MUTUAL_EXCHANGE"].split(',')


#[data_extractor]
symbology_sources = config_object["data_extractor"]["symbology_sources"].split(',')
FORCE_DELETE_ETL = config_object["data_extractor"]["delete_file_etl"]
WORKER_SLEEP_TIME = config_object["data_extractor"]["worker_sleep_time"]

#[price_extractor]
try:
    THREADS = int(config_object["price_extractor"]["THREADS"])
except:
    THREADS = 4

