from decouple import config

LOG_LEVEL = config("LOG_LEVEL", default="INFO")
TOKEN = config("TOKEN")
INSERTER_MAX_RETRIES = config("INSERTER_MAX_RETRIES", default=3, cast=int)
REQUEST_MAX_RETRIES = config("REQUEST_MAX_RETRIES", default=3, cast=int)
REQUEST_BACKOFF_FACTOR = config("REQUEST_BACKOFF_FACTOR", default=2, cast=int)
MSSQL_AD_LOGIN = config("MSSQL_AD_LOGIN", cast=bool, default=False)
MSSQL_SERVER = config("MSSQL_SERVER")
MSSQL_DATABASE = config("MSSQL_DATABASE")

if not MSSQL_AD_LOGIN:
    MSSQL_USERNAME = config("MSSQL_USERNAME")
    MSSQL_PASSWORD = config("MSSQL_PASSWORD")
