from chalicelib.new.config.infra import envars

REGIMEN_FISCAL_MODEL_BY_VERSION = {
    "expand": "tequitl.cat.regimen_fiscal",
    "tequitl": "tq.cat.regimen_fiscal",
}

REGIMEN_FISCAL_FIELD_BY_VERSION = {
    "expand": "c_regimen_fiscal_id",
    "tequitl": "tq_tax_regime_id",
}


def get_regimen_fiscal_model() -> str:
    return REGIMEN_FISCAL_MODEL_BY_VERSION[envars.odoo.version]


def get_regimen_fiscal_field() -> str:
    return REGIMEN_FISCAL_FIELD_BY_VERSION[envars.odoo.version]
