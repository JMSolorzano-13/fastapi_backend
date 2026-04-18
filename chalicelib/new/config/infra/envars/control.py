import os
from datetime import datetime, timedelta

PARALLEL_VERIFICATIONS = int(os.environ.get("PARALLEL_VERIFICATIONS", 15))
MAX_SAT_WS_REQUEST_TIMEOUT = int(os.environ.get("MAX_SAT_WS_REQUEST_TIMEOUT", 60))
MAX_SAT_WS_DOWNLOAD_TIMEOUT = int(os.environ.get("MAX_SAT_WS_REQUEST_TIMEOUT", 30))
MAX_DAYS_FOR_CHUNK = int(os.environ.get("MAX_DAYS_FOR_CHUNK", 180))
PARALLEL_SENDS = int(os.environ.get("PARALLEL_VERIFICATIONS", 15))
MAX_SAT_WS_REQUEST_SEND_TIMEOUT = int(os.environ.get("MAX_SAT_WS_REQUEST_TIMEOUT", 60))
MAX_HOURS_WITHOUT_METADATA_FOR_COMPLETE = timedelta(
    hours=int(os.environ.get("MAX_HOURS_WITHOUT_METADATA_FOR_COMPLETE", 3))
)
XML_CREATE_RECORDS = bool(int(os.environ.get("XML_CREATE_RECORDS", 0)))
FUZZY_SEARCH_ACTIVE = bool(int(os.environ.get("FUZZY_SEARCH_ACTIVE", 1)))
MAX_CFDI_QTY_IN_QUERY = 200_000
CHECK_NEW_CFDI_DELTA = timedelta(days=10)
MIN_CFDI_DATE = datetime.fromisoformat("2017-01-01")
CSV_COLUMN_LIMIT_MB = int(os.environ.get("CSV_COLUMN_LIMIT_MB", 1000))
NUM_QUERY_SPLITS = int(os.environ.get("NUM_QUERY_SPLITS", 64))

# ISR
ISR_DEFAULT_PERCENTAGE = 0.47
ISR_PERCENTAGE_LIST = {ISR_DEFAULT_PERCENTAGE, 0.53}

ADMIN_EMAILS = ["admin@sg.com", "main@test.com"]
ADMIN_CREATE_DEFAULT_LICENSE = {
    "id": 1,
    "date_start": "2025-07-08",
    "date_end": "2035-07-08",
    "details": {
        "max_emails_enroll": "unlimited",
        "max_companies": "unlimited",
        "exceed_metadata_limit": False,
        "add_enabled": False,
        "products": [{"identifier": "prod_MZAVa4wGwDTZJ9", "quantity": 1}],
    },
    "stripe_status": "active",
}
MANUAL_REQUEST_START_DELTA = timedelta(hours=72)

COI_PREFIX = "coi"
COI_METADATA_SUFFIX = "metadata.csv"
COI_DATA_SUFFIX = "data.zip"
COI_CANCEL_SUFFIX = "cancel.csv"
