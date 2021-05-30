# coding: utf-8
class Config(object):
    DEBUG = False
    TESTING = False
    SAP_DB_SERVER_IP="192.168.11.249"
    MSSQL_USER="sa"
    MSSQL_PASS="sboSIS&9741"
    SIS_DB_NAME="SBO_SIS"
    SUPPLIERS_INFO_FILE="mailing_list_fournisseurs_locaux.csv"
    SQLITE_TAGS_DB='database_sqlite.db'
    ITEMCODE_REGEX=r'^\d{6}$'
    CODEBARS_REGEX=r'^\d{7,14}$'
    IMPORT_DIRECTORY_PATH=""
    ITEMS_IMPORT_FILE="DOSSIERS_IMPORT_MAJ041017.xls"
    APP_RUNNING_PORT=5000
    GOOGLE_CREDENTIALS="credentials.json"

class ProductionConfig(Config):
    pass

class DevelopmentConfig(Config):
    DEBUG = True

class TestingConfig(Config):
    TESTING = True
