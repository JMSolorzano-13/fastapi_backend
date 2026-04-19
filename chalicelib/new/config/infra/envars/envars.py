import json
import os
from datetime import datetime, timedelta

from dotenv import load_dotenv

load_dotenv()

REGION_NAME = "us-east-1"
GENERIC_TIMEOUT = int(os.environ.get("GENERIC_TIMEOUT", 10))


DEFAULT_LICENSE = os.environ.get("DEFAULT_LICENSE", "datastripe")
# Pasto
PASTO_REQUEST_TIMEOUT = int(os.environ.get("PASTO_REQUEST_TIMEOUT", 50))
PASTO_MAX_RETRIES = int(os.environ.get("PASTO_MAX_RETRIES", 1))

WKHTMLTOPDF_PATH = os.environ.get("WKHTMLTOPDF_PATH", "/opt/bin/wkhtmltopdf")

# Crons
SYNC_METADATA_CRON = "cron(00 00 * * ? *)"

EFOS_CRON = "cron(00 00 * * ? *)"
NOTIFICATION_EMAIL_CRON = "cron(00 00 * * ? *)"
PROCESS_DELAYED_CRON = "cron(00 00 * * ? *)"
REVERIFY_CRON = "cron(00 00 * * ? *)"
COMPLETE_CFDIS_WITHOUT_METADATA = "cron(00 00 * * ? *)"
# SCRAP CRON
SCRAP_CRON = "cron(00 00 * * ? *)"

MARKETING_EMAIL_CRON = "cron(00 00 * * ? *)"

# CLEAN SAT QUERY TABLE
CLEAN_SAT_QUERY_TABLE_CRON = "cron(00 00 * * ? *)"
DAYS_TO_KEEP_SAT_QUERY_TABLE = int(os.environ.get("DAYS_TO_KEEP_SAT_QUERY_TABLE", 7))

# Re-Send old
REVERIFY_CREATED_AFTER = timedelta(hours=int(os.environ.get("REVERIFY_CREATED_AFTER_X_HOURS", 48)))
REVERIFY_CREATED_BEFORE = timedelta(
    hours=int(os.environ.get("REVERIFY_CREATED_BEFORE_X_HOURS", 24))
)

# Email Notifications
DEFAULT_LAST_NOTIFICATION_DATE = datetime.fromisoformat("2000-01-01")
TIMEDELTA_ISSUE_WINDOW = timedelta(days=0)
MAX_RESULTS_PER_SECTION = 11

# Process delayed
MAX_CPU_UTILIZATION = 70

# SQSdata_queue_metadata_dev
SQS_PROCESS_PACKAGE_METADATA = os.environ["SQS_PROCESS_PACKAGE_METADATA"]
SQS_PROCESS_PACKAGE_XML = os.environ["SQS_PROCESS_PACKAGE_XML"]
SQS_COMPLETE_CFDIS = os.environ["SQS_COMPLETE_CFDIS"]
SQS_VERIFY_QUERY = os.environ["SQS_VERIFY_QUERY"]
SQS_SEND_QUERY_METADATA = os.environ["SQS_SEND_QUERY_METADATA"]
SQS_DOWNLOAD_QUERY = os.environ["SQS_DOWNLOAD_QUERY"]
SQS_CREATE_QUERY = os.environ["SQS_CREATE_QUERY"]
SQS_UPDATER_QUERY = os.environ["SQS_UPDATER_QUERY"]
# S3
S3_CERTS = os.environ["S3_CERTS"]
S3_ATTACHMENTS = os.environ["S3_ATTACHMENTS"]
S3_EXPORT = os.environ["S3_EXPORT"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]
S3_FILESATTACH = os.environ["S3_FILESATTACH"]

# Log & Debug
DEV_MODE = bool(int(os.environ.get("DEV_MODE", 0)))
DEV_FIEL_AND_CSD_PASSPHRASE = os.environ.get("DEV_FIEL_AND_CSD_PASSPHRASE", "").encode()
LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG")
DB_LOG_LEVEL = os.environ.get("DB_LOG_LEVEL", "WARNING")
LOCAL_INFRA = bool(int(os.environ.get("LOCAL_INFRA", 0)))

# ODOO
ODOO_URL = os.environ.get("ODOO_URL")
ODOO_DB = os.environ.get("ODOO_DB")
ODOO_USER = os.environ.get("ODOO_USER")
ODOO_PASSWORD = os.environ.get("ODOO_PASSWORD")
ODOO_PORT = int(os.environ.get("ODOO_PORT", 443))
NOTIFY_ODOO = bool(int(os.environ.get("NOTIFY_ODOO", True)))


# SES
SES_MAIL = os.environ["SES_MAIL"]

# Auth: Cognito IDP (default) vs local JWT POC (no Cognito at runtime)
_AUTH_BACKEND_RAW = os.environ.get("AUTH_BACKEND", "cognito").strip().lower()
if _AUTH_BACKEND_RAW not in ("cognito", "local_jwt"):
    raise ValueError(
        f"AUTH_BACKEND must be 'cognito' or 'local_jwt', got {_AUTH_BACKEND_RAW!r}"
    )
AUTH_BACKEND = _AUTH_BACKEND_RAW

if AUTH_BACKEND == "local_jwt" and not LOCAL_INFRA and not os.environ.get("JWT_SECRET", "").strip():
    raise ValueError(
        "AUTH_BACKEND=local_jwt requires JWT_SECRET when LOCAL_INFRA=0 "
        "(configure in Azure Key Vault / ACA secrets)"
    )

# Cognito
if AUTH_BACKEND == "local_jwt":
    COGNITO_USER_POOL_ID = os.environ.get("COGNITO_USER_POOL_ID")
    COGNITO_CLIENT_ID = os.environ.get("COGNITO_CLIENT_ID", "")
    COGNITO_CLIENT_SECRET = os.environ.get("COGNITO_CLIENT_SECRET")
    if COGNITO_CLIENT_SECRET == "N/A":
        COGNITO_CLIENT_SECRET = None
    COGNITO_REDIRECT_URI = os.environ.get("COGNITO_REDIRECT_URI", "http://localhost:5173/callback")
    COGNITO_URL = os.environ.get("COGNITO_URL", "https://cognito-local-jwt-placeholder.invalid/")
else:
    COGNITO_USER_POOL_ID = os.environ.get("COGNITO_USER_POOL_ID")
    COGNITO_CLIENT_ID = os.environ["COGNITO_CLIENT_ID"]
    COGNITO_CLIENT_SECRET = os.environ.get("COGNITO_CLIENT_SECRET")
    if COGNITO_CLIENT_SECRET == "N/A":
        COGNITO_CLIENT_SECRET = None
    COGNITO_REDIRECT_URI = os.environ.get("COGNITO_REDIRECT_URI", "http://localhost:5173/callback")
    COGNITO_URL = os.environ["COGNITO_URL"]
DISPLAY_LOGO_IN_CFDI_PDF = not bool(COGNITO_CLIENT_SECRET)

# DB
DB_HOST = os.environ["DB_HOST"]
DB_HOST_RO = os.environ.get("DB_HOST_RO", DB_HOST)
DB_PORT = os.environ.get("DB_PORT", 5432)
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

# SAT WS
WS_MAX_WAITING_MINUTES = timedelta(minutes=int(os.environ.get("WS_MAX_WAITING_MINUTES", 60)))
WS_MAX_WAITING_MINUTES_TO_RECREATE = timedelta(
    minutes=int(os.environ.get("WS_MAX_WAITING_MINUTES_TO_RECREATE", 60 * 5))
)
MAX_CFDI_PER_CHUNK = 6_950

# Misc
FRONTEND_BASE_URL = os.environ.get("FRONTEND_BASE_URL", "http://localhost:5173")
IS_SIIGO = bool(int(os.environ.get("IS_SIIGO", False)))
if COGNITO_CLIENT_SECRET:
    IS_SIIGO = True
SECURITY_GROUP = os.environ.get("SECURITY_GROUP", "")

# Scraper
SQS_SCRAP_ORCHESTRATOR = os.environ["SQS_SCRAP_ORCHESTRATOR"]  # SQS que apunta al scraper
SQS_SCRAP_DELAYER = os.environ["SQS_SCRAP_DELAYER"]
SQS_SCRAP_RESULTS = os.environ["SQS_SCRAP_RESULTS"]
S3_UUIDS_COMPARE_SCRAPER = os.environ["S3_UUIDS_COMPARE_SCRAPER"]
SCRAPER_S3_BUCKET = S3_ATTACHMENTS

SCRAP_MANUAL_START_DATE = datetime.fromisoformat(
    os.environ.get("SCRAP_MANUAL_STARTDATE", "2025-01-01")
)

# Cloudwatch
STATISTICS_INFO_TIME_DELTA = timedelta(
    minutes=int(os.environ.get("STATISTICS_INFO_TIME_DELTA", 10))
)
STATISTICS_INFO_PERIOD_SECONDS = int(os.environ.get("STATISTICS_INFO_PERIOD_SECONDS", 60))
DB_CLUSTER_IDENTIFIER = os.environ["DB_CLUSTER_IDENTIFIER"]

# Scrap
SQS_SAT_SCRAP_PDF = os.environ["SQS_SAT_SCRAP_PDF"]
_scrap_start = os.environ.get("SCRAP_START_METADATA_CANCEL")
SCRAP_START_METADATA_CANCEL = (
    datetime.fromisoformat(_scrap_start) if _scrap_start and _scrap_start.strip() else None
)
SCRAP_DATA_IN_CRON = bool(int(os.environ.get("SCRAP_DATA_IN_CRON", 1)))

# Export
SQS_EXPORT = os.environ["SQS_EXPORT"]
SQS_MASSIVE_EXPORT = os.environ["SQS_MASSIVE_EXPORT"]
# Notifications
SQS_NOTIFICATIONS = os.environ["SQS_NOTIFICATIONS"]
# License
DEFAULT_LICENSE_LIFETIME = timedelta(days=int(os.environ.get("DEFAULT_LICENSE_LIFETIME", 10)))

MAX_FILE_SIZE_KB = int(os.environ.get("MAX_FILE_SIZE_KB", 500))

# Stripe
NOTIFY_STRIPE = bool(int(os.environ.get("NOTIFY_STRIPE", True)))
STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY")
STRIPE_DEFAULT_ITEMS = json.loads(os.environ.get("STRIPE_DEFAULT_ITEMS", "[]"))
STRIPE_COUPON = os.environ.get("STRIPE_COUPON", "")
STRIPE_DEFAULT_TAX_RATES = json.loads(os.environ.get("STRIPE_DEFAULT_TAX_RATES", "[]"))
STRIPE_DAYS_UNTIL_DUE = int(os.environ.get("STRIPE_DAYS_UNTIL_DUE", 3))
STRIPE_SET_PRODUCT_SECRET_KEY = os.environ.get("STRIPE_SET_PRODUCT_SECRET_KEY", "")
STRIPE_DEFAULT_PRORATION_BEHAVIOR = "always_invoice"
STRIPE_DEFAULT_PRORATION_BEHAVIOR_CURRENT_CUSTOMERS = "none"
# Default trial/cancel delta for Stripe subscriptions, defined in code only
STRIPE_DEFAULT_CANCEL_AT_DELTA = timedelta(days=15)
STRIPE_WEBHOOK_PAID_ALERT = os.environ.get("STRIPE_WEBHOOK_PAID_ALERT", "")

# DEV
DATE_FREEZED = datetime.fromisoformat("2022-02-13")
DATETIME_FREEZED = datetime.fromisoformat("2022-02-13 05:10:00")
DATETIME_FREEZED_TO_RETRY = datetime.fromisoformat("2022-02-12 12:10:00")


MAX_MANUAL_SYNC_PER_DAY = int(os.environ.get("MAX_MANUAL_SYNC_PER_DAY", 5))

PERSIST_TESTS = bool(os.environ.get("PERSIST_TESTS", False))
ALEMBIC_TEST = bool(os.environ.get("ALEMBIC_TEST", False))

# Pasto
PASTO_URL = os.environ["PASTO_URL"]
PASTO_OCP_KEY = os.environ["PASTO_OCP_KEY"]
PASTO_EMAIL = os.environ["PASTO_EMAIL"]
PASTO_PASSWORD = os.environ["PASTO_PASSWORD"]
PASTO_RESET_LICENSE_URL = os.environ["PASTO_RESET_LICENSE_URL"]
PASTO_SUBSCRIPTION_ID = os.environ["PASTO_SUBSCRIPTION_ID"]
PASTO_DASHBOARD_ID = os.environ["PASTO_DASHBOARD_ID"]
SQS_PASTO_CONFIG_WORKER = os.environ["SQS_PASTO_CONFIG_WORKER"]
SQS_PASTO_GET_COMPANIES = os.environ["SQS_PASTO_GET_COMPANIES"]
SQS_ADD_PROCESS_METADATA = os.environ["SQS_PASTO_PROCESS_METADATA"]
SQS_ADD_DATA_SYNC = os.environ["SQS_PASTO_FULL_SYNC"]
SQS_RESET_ADD_LICENSE_KEY = os.environ["SQS_RESET_ADD_LICENSE_KEY"]
ADD_CONFIG_WEBHOOK = "Pasto/Config"
ADD_COMPANIES_WEBHOOK = "Pasto/Company"
ADD_METADATA_WEBHOOK = "Pasto/Metadata"
ADD_XML_WEBHOOK = "Pasto/XML"
ADD_CANCEL_WEBHOOK = "Pasto/Cancel"

# Mock

# ADD
SQS_ADD_METADATA_REQUEST = os.environ["SQS_ADD_SYNC_METADATA"]
ADD_METADATA_SYNC_CRON = "cron(00 00 * * ? *)"
S3_ADD = os.environ["S3_ADD"]
ADD_S3_EXPIRATION_DELTA = timedelta(days=int(os.environ.get("ADD_S3_EXPIRATION_DELTA", 7)))

# Self
SELF_ENDPOINT = os.environ["VITE_REACT_APP_BASE_URL"]

# True when the SPA is configured to call an API on this machine (Vite :5173 + FastAPI :8001).
_VITE_BASE_LOWER = (os.environ.get("VITE_REACT_APP_BASE_URL") or "").lower()
LOCAL_DEV_API = bool(
    LOCAL_INFRA
    or ("localhost" in _VITE_BASE_LOWER)
    or ("127.0.0.1" in _VITE_BASE_LOWER)
    or ("[::1]" in _VITE_BASE_LOWER)
)

# VITE
VITE_REACT_APP_PRODUCT_TRIAL = os.environ.get("VITE_REACT_APP_PRODUCT_TRIAL")
VITE_APP_LOGO_URL = os.environ.get("VITE_APP_LOGO_URL")

# BLOCK
BLOCK_APP_ACCESS = bool(int(os.environ.get("BLOCK_APP_ACCESS", 0)))
BLOCK_APP_MESSAGE = os.environ.get(
    "BLOCK_APP_MESSAGE",
    "Estamos trabajando para mejorar el sitio. Intenta acceder en un momento más...",
)

# TRIAL ACCOUNT
MAX_SAME_COMPANY_IN_TRIALS = int(os.environ.get("MAX_SAME_COMPANY_IN_TRIALS", 2))


# TESTS
PERSIST_TESTS = bool(int(os.environ.get("PERSIST_TESTS", 0)))
ALEMBIC_TESTS = bool(int(os.environ.get("ALEMBIC_TESTS", 0)))

# COI
SQS_COI_DATA_SYNC = "coi_data_sync"
SQS_COI_METADATA_UPLOADED = "coi_metadata_uploaded"

# Siigo FreeTrial
SIIGO_FREETRIAL_BASE_URL = os.environ.get(
    "PASTO_URL",
    "https://servicesqamx.siigo.com/strategy/api/backoffice-company/freeTrial",
)
SIIGO_FREETRIAL_TIMEOUT = int(os.environ.get("SIIGO_FREETRIAL_TIMEOUT", 50))
