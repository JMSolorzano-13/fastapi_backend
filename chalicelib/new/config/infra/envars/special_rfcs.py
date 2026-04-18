SPECIAL_RFCS = {
    "PGD1009214W0",
    "CPL151127NR7",
}

SPECIAL_COMPANIES_SCRAP_CRON = {"PGD1009214W0": {"750e799c-6990-4e25-a3c4-228f6d276640"}}

MOCK_PACKAGES = {
    "PGD1009214W0": {
        "METADATA": {
            "ISSUED": ("PGD1009214W0/metadata_issued",),
            "RECEIVED": ("PGD1009214W0/metadata_received",),
        },
        "CFDI": {
            "ISSUED": tuple(f"PGD1009214W0/cfdi_issued_{i}" for i in range(1, 8)),
            "RECEIVED": tuple(f"PGD1009214W0/cfdi_received_{i}" for i in range(1, 7)),
        },
    },
}
