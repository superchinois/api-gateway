# coding: utf-8
from dotenv import dotenv_values

def env_config():
    return dotenv_values(".env")

class Config(object):
    DEBUG = False
    TESTING = False
    SAP_DB_SERVER_IP="192.168.11.249"
    SIS_DB_NAME="SBO_SIS"
    SUPPLIERS_INFO_FILE="mailing_list_fournisseurs_locaux.csv"
    SQLITE_TAGS_DB='database_sqlite.db'
    ITEMCODE_REGEX=r'^\d{6}$'
    CODEBARS_REGEX=r'^\d{7,14}$'
    IMPORT_DIRECTORY_PATH=""
    ITEMS_IMPORT_FILE="DOSSIERS_IMPORT_MAJ041017.xls"
    APP_RUNNING_PORT=5000
    GOOGLE_CREDENTIALS="credentials.json"
    SHEET_ID="19JOyp4S78mfTKsAClaSWMDhppbrsbyHl58hvBd_1pMo"

    @property
    def MSSQL_USER(self):
        return env_config()["MSSQL_USER"]
    @property
    def MSSQL_PASS():
        return env_config()["MSSQL_PASS"]

class ProductionConfig(Config):
    pass

class DevelopmentConfig(Config):
    DEBUG = True

class TestingConfig(Config):
    TESTING = True
