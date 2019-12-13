from starlette.config import Config

config = Config('.env')

DB_HOST = config("DB_HOST", cast=str, default="localhost")
DB_USERNAME = config("DB_USERNAME", cast=str, default="root")
DB_PASSWORD = config("DB_PASSWORD", cast=str, default="root")
DB_NAME = config("DB_NAME", cast=str, default="default")

SLACK_WEBHOOK_URL = config("SLACK_WEBHOOK_URL", cast=str, default="slack.com")

HIVE_URL = config("HIVE_URL", cast=str, default="localhost:2181")
PROXY_USER = config("PROXY_USER", cast=str, default="123456")

TEMP_PATH = config("TEMP_PATH", cast=str, default="/var/tmp")