# coding: utf-8
from dotenv import dotenv_values

def env_config():
    return dotenv_values(".env")

class Config(object):
    DEBUG = False
    TESTING = False
    SAP_DB_SERVER_IP="192.168.11.249"
    MSSQL_USER=env_config()["MSSQL_USER"]
    MSSQL_PASS=env_config()["MSSQL_PASS"]
    SIS_DB_NAME="SBO_SIS"
    SUPPLIERS_INFO_FILE="mailing_list_fournisseurs_locaux.csv"
    RICE_MASTER_FILE="rice_master_data.csv"
    SQLITE_TAGS_DB='database_sqlite.db'
    ITEMCODE_REGEX=r'^\d{6}$'
    CODEBARS_REGEX=r'^\d{7,14}$'
    IMPORT_DIRECTORY_PATH=""
    ITEMS_IMPORT_FILE="DOSSIERS_IMPORT_MAJ041017.xls"
    APP_RUNNING_PORT=5000
    GOOGLE_CREDENTIALS="credentials.json"
    SHEET_ID="19JOyp4S78mfTKsAClaSWMDhppbrsbyHl58hvBd_1pMo"
    MONGO_URI="mongodb://{mongo_server}:{mongo_port}".format(mongo_server=env_config()["MONGO_SERVER"]
                                                            ,mongo_port=env_config()["MONGO_PORT"])
    MONGO_DATABASE="analytics"
    MONGO_COLLECTION="raw-data"

class ProductionConfig(Config):
    pass

class DevelopmentConfig(Config):
    DEBUG = True

class TestingConfig(Config):
    TESTING = True
