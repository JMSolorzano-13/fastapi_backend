"""tenant_per_schema

Revision ID: d36bba504e9c
Revises:
Create Date: 2025-09-03 15:27:37.418881

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from chalicelib.alembic_tenant.utils import resolve_schema, schema_to_uuid
from chalicelib.schema.models.model import SHARED_TENANT_SCHEMA_PLACEHOLDER

# revision identifiers, used by Alembic.
revision = "d36bba504e9c"
down_revision = "7018e946a0f2"
branch_labels = None
depends_on = None
add_sync_request_state_enum = postgresql.ENUM(
    "DRAFT",
    "SENT",
    "ERROR",
    name="add_sync_request_state_enum",
    schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
)

cfdiexportstate = postgresql.ENUM(
    "SENT",
    "TO_DOWNLOAD",
    "ERROR",
    name="cfdiexportstate",
    schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
)

exportdatatype = postgresql.ENUM(
    "CFDI",
    "IVA",
    "ISR",
    name="exportdatatype",
    schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
)

version = postgresql.ENUM(
    "1.1",
    "1.2",
    name="version",
    schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
)

tiponomina = postgresql.ENUM(
    "O",
    "E",
    name="tiponomina",
    schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
)

receptortipocontrato = postgresql.ENUM(
    "01",
    "02",
    "03",
    "04",
    "05",
    "06",
    "07",
    "08",
    "09",
    "10",
    "99",
    name="receptortipocontrato",
    schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
)

receptortipojornada = postgresql.ENUM(
    "01",
    "02",
    "03",
    "04",
    "05",
    "06",
    "07",
    "08",
    "99",
    name="receptortipojornada",
    schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
)

receptortiporegimen = postgresql.ENUM(
    "02",
    "03",
    "04",
    "05",
    "06",
    "07",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "99",
    name="receptortiporegimen",
    schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
)

receptorriesgopuesto = postgresql.ENUM(
    "1",
    "2",
    "3",
    "4",
    "5",
    "99",
    name="receptorriesgopuesto",
    schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
)

receptorperiodicidadpago = postgresql.ENUM(
    "01",
    "02",
    "03",
    "04",
    "05",
    "06",
    "07",
    "08",
    "09",
    "10",
    "99",
    name="receptorperiodicidadpago",
    schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
)

receptorbanco = postgresql.ENUM(
    "002",
    "006",
    "009",
    "012",
    "014",
    "019",
    "021",
    "030",
    "032",
    "036",
    "037",
    "042",
    "044",
    "058",
    "059",
    "060",
    "062",
    "072",
    "102",
    "103",
    "106",
    "108",
    "110",
    "112",
    "113",
    "116",
    "124",
    "126",
    "127",
    "128",
    "129",
    "130",
    "131",
    "132",
    "133",
    "134",
    "135",
    "136",
    "137",
    "138",
    "139",
    "140",
    "141",
    "143",
    "145",
    "147",
    "148",
    "149",
    "150",
    "151",
    "152",
    "153",
    "154",
    "155",
    "156",
    "157",
    "158",
    "159",
    "160",
    "166",
    "168",
    "600",
    "601",
    "602",
    "605",
    "606",
    "607",
    "608",
    "610",
    "614",
    "615",
    "616",
    "617",
    "618",
    "619",
    "620",
    "621",
    "622",
    "623",
    "626",
    "627",
    "628",
    "629",
    "630",
    "631",
    "632",
    "633",
    "634",
    "636",
    "637",
    "638",
    "640",
    "642",
    "646",
    "647",
    "648",
    "649",
    "651",
    "652",
    "653",
    "655",
    "656",
    "659",
    "670",
    "901",
    "902",
    name="receptorbanco",
    schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
)

receptorclaveentfed = postgresql.ENUM(
    "AGU",
    "BCN",
    "BCS",
    "CAM",
    "CHP",
    "CHH",
    "COA",
    "COL",
    "CMX",
    "DIF",
    "DUR",
    "GUA",
    "GRO",
    "HID",
    "JAL",
    "MEX",
    "MIC",
    "MOR",
    "NAY",
    "NLE",
    "OAX",
    "PUE",
    "QUE",
    "ROO",
    "SLP",
    "SIN",
    "SON",
    "TAB",
    "TAM",
    "TLA",
    "VER",
    "YUC",
    "ZAC",
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "NC",
    "SC",
    "CO",
    "CT",
    "ND",
    "SD",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NJ",
    "NY",
    "NH",
    "NM",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WV",
    "WA",
    "WI",
    "WY",
    "ON",
    "QC",
    "NS",
    "NB",
    "MB",
    "BC",
    "PE",
    "SK",
    "AB",
    "NL",
    "NT",
    "YT",
    "UN",
    name="receptorclaveentfed",
    schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
)

downloadtype = postgresql.ENUM(
    "ISSUED",
    "RECEIVED",
    "BOTH",
    name="downloadtype",
    schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
)

requesttype = postgresql.ENUM(
    "CFDI",
    "METADATA",
    "BOTH",
    "CANCELLATION",
    name="requesttype",
    schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
)

querystate = postgresql.ENUM(
    "DRAFT",
    "SENT",
    "TO_DOWNLOAD",
    "DOWNLOADED",
    "TO_SCRAP",
    "DELAYED",
    "PROCESSING",
    "ERROR_IN_CERTS",
    "ERROR_SAT_WS_UNKNOWN",
    "ERROR_SAT_WS_INTERNAL",
    "ERROR_TOO_BIG",
    "TIME_LIMIT_REACHED",
    "ERROR",
    "SCRAP_FAILED",
    "CANT_SCRAP",
    "MANUALLY_CANCELLED",
    "SPLITTED",
    "INFORMATION_NOT_FOUND",
    "SUBSTITUTED",
    "SUBSTITUTED_TO_SCRAP",
    "PROCESSED",
    "SCRAPPED",
    name="querystate",
    schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
)

satdownloadtechnology = postgresql.ENUM(
    "WebService",
    "Scraper",
    name="satdownloadtechnology",
    schema=SHARED_TENANT_SCHEMA_PLACEHOLDER,
)


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    bind = op.get_bind()
    add_sync_request_state_enum.create(bind=bind, checkfirst=True)
    cfdiexportstate.create(bind=bind, checkfirst=True)
    exportdatatype.create(bind=bind, checkfirst=True)
    version.create(bind=bind, checkfirst=True)
    tiponomina.create(bind=bind, checkfirst=True)
    receptortipocontrato.create(bind=bind, checkfirst=True)
    receptortipojornada.create(bind=bind, checkfirst=True)
    receptortiporegimen.create(bind=bind, checkfirst=True)
    receptorriesgopuesto.create(bind=bind, checkfirst=True)
    receptorperiodicidadpago.create(bind=bind, checkfirst=True)
    receptorbanco.create(bind=bind, checkfirst=True)
    receptorclaveentfed.create(bind=bind, checkfirst=True)
    downloadtype.create(bind=bind, checkfirst=True)
    requesttype.create(bind=bind, checkfirst=True)
    querystate.create(bind=bind, checkfirst=True)
    satdownloadtechnology.create(bind=bind, checkfirst=True)

    tenant_schema = resolve_schema("per_tenant")
    op.alter_column(
        "add_sync_request",
        "state",
        schema=tenant_schema,
        type_=add_sync_request_state_enum,
        postgresql_using="state::text::public.add_sync_request_state_enum",
    )
    op.add_column(
        "cfdi",
        sa.Column(
            "company_identifier",
            postgresql.UUID(),
            nullable=False,
            server_default=schema_to_uuid(tenant_schema),
        ),
        schema=tenant_schema,
        if_not_exists=True,
    )
    op.drop_constraint("cfdi_pkey", table_name="cfdi", schema=tenant_schema, type_="primary")
    op.create_primary_key(
        "cfdi_pkey", "cfdi", ["company_identifier", "is_issued", "UUID"], schema=tenant_schema
    )
    op.alter_column(
        "cfdi_export",
        "state",
        schema=tenant_schema,
        type_=cfdiexportstate,
        postgresql_using="state::text::public.cfdiexportstate",
    )
    op.alter_column(
        "cfdi_export",
        "export_data_type",
        schema=tenant_schema,
        server_default=None,
    )
    op.alter_column(
        "cfdi_export",
        "export_data_type",
        schema=tenant_schema,
        type_=exportdatatype,
        postgresql_using="export_data_type::text::public.exportdatatype",
        server_default="CFDI",
    )

    op.add_column(
        "cfdi_relation",
        sa.Column(
            "company_identifier",
            postgresql.UUID(),
            nullable=False,
            server_default=schema_to_uuid(tenant_schema),
        ),
        schema=tenant_schema,
        if_not_exists=True,
    )
    op.drop_constraint(
        "cfdi_relation_pkey", table_name="cfdi_relation", schema=tenant_schema, type_="primary"
    )
    op.create_primary_key(
        "cfdi_relation_pkey",
        "cfdi_relation",
        ["company_identifier", "is_issued", "identifier"],
        schema=tenant_schema,
    )

    op.add_column(
        "nomina",
        sa.Column(
            "company_identifier",
            postgresql.UUID(),
            nullable=False,
            server_default=schema_to_uuid(tenant_schema),
        ),
        schema=tenant_schema,
        if_not_exists=True,
    )
    op.drop_constraint("nomina_pkey", table_name="nomina", schema=tenant_schema, type_="primary")
    op.create_primary_key(
        "nomina_pkey",
        "nomina",
        ["company_identifier", "cfdi_uuid"],
        schema=tenant_schema,
    )
    op.alter_column(
        "nomina",
        "Version",
        schema=tenant_schema,
        type_=version,
        postgresql_using='"Version"::text::public.version',
    )
    op.alter_column(
        "nomina",
        "TipoNomina",
        schema=tenant_schema,
        type_=tiponomina,
        postgresql_using='"TipoNomina"::text::public.tiponomina',
    )
    op.alter_column(
        "nomina",
        "ReceptorTipoContrato",
        schema=tenant_schema,
        type_=receptortipocontrato,
        postgresql_using='"ReceptorTipoContrato"::text::public.receptortipocontrato',
    )
    op.alter_column(
        "nomina",
        "ReceptorTipoJornada",
        schema=tenant_schema,
        type_=receptortipojornada,
        postgresql_using='"ReceptorTipoJornada"::text::public.receptortipojornada',
    )
    op.alter_column(
        "nomina",
        "ReceptorTipoRegimen",
        schema=tenant_schema,
        type_=receptortiporegimen,
        postgresql_using='"ReceptorTipoRegimen"::text::public.receptortiporegimen',
    )
    op.alter_column(
        "nomina",
        "ReceptorRiesgoPuesto",
        schema=tenant_schema,
        type_=receptorriesgopuesto,
        postgresql_using='"ReceptorRiesgoPuesto"::text::public.receptorriesgopuesto',
    )
    op.alter_column(
        "nomina",
        "ReceptorPeriodicidadPago",
        schema=tenant_schema,
        type_=receptorperiodicidadpago,
        postgresql_using='"ReceptorPeriodicidadPago"::text::public.receptorperiodicidadpago',
    )
    op.alter_column(
        "nomina",
        "ReceptorClaveEntFed",
        schema=tenant_schema,
        type_=receptorclaveentfed,
        postgresql_using='"ReceptorClaveEntFed"::text::public.receptorclaveentfed',
    )
    op.alter_column(
        "nomina",
        "ReceptorBanco",
        schema=tenant_schema,
        type_=receptorbanco,
        postgresql_using='"ReceptorBanco"::text::public.receptorbanco',
    )

    op.add_column(
        "payment",
        sa.Column(
            "company_identifier",
            postgresql.UUID(),
            nullable=False,
            server_default=schema_to_uuid(tenant_schema),
        ),
        schema=tenant_schema,
        if_not_exists=True,
    )
    op.drop_constraint("payment_pkey", table_name="payment", schema=tenant_schema, type_="primary")
    op.create_primary_key(
        "payment_pkey",
        "payment",
        ["company_identifier", "identifier"],
        schema=tenant_schema,
    )

    op.add_column(
        "payment_relation",
        sa.Column(
            "company_identifier",
            postgresql.UUID(),
            nullable=False,
            server_default=schema_to_uuid(tenant_schema),
        ),
        schema=tenant_schema,
        if_not_exists=True,
    )
    op.drop_constraint(
        "payment_relation_pkey",
        table_name="payment_relation",
        schema=tenant_schema,
        type_="primary",
    )
    op.create_primary_key(
        "payment_relation_pkey",
        "payment_relation",
        ["company_identifier", "identifier"],
        schema=tenant_schema,
    )

    op.alter_column(
        "sat_query",
        "download_type",
        schema=tenant_schema,
        type_=downloadtype,
        postgresql_using="download_type::text::public.downloadtype",
    )
    op.alter_column(
        "sat_query",
        "request_type",
        schema=tenant_schema,
        type_=requesttype,
        postgresql_using="request_type::text::public.requesttype",
    )
    op.alter_column(
        "sat_query",
        "state",
        schema=tenant_schema,
        type_=querystate,
        postgresql_using="state::text::public.querystate",
    )

    op.alter_column(
        "sat_query",
        "technology",
        schema=tenant_schema,
        server_default=None,
    )
    op.alter_column(
        "sat_query",
        "technology",
        schema=tenant_schema,
        type_=satdownloadtechnology,
        postgresql_using="technology::text::public.satdownloadtechnology",
        server_default="WebService",
    )

    op.execute(f'DROP TYPE IF EXISTS "{tenant_schema}"."enum_add_sync_request_state"')
    op.execute(f'DROP TYPE IF EXISTS "{tenant_schema}"."cfdiexportstate"')
    op.execute(f'DROP TYPE IF EXISTS "{tenant_schema}"."exportdatatype"')
    op.execute(f'DROP TYPE IF EXISTS "{tenant_schema}"."enum_nomina_Version"')
    op.execute(f'DROP TYPE IF EXISTS "{tenant_schema}"."enum_nomina_TipoNomina"')
    op.execute(f'DROP TYPE IF EXISTS "{tenant_schema}"."enum_nomina_ReceptorTipoContrato"')
    op.execute(f'DROP TYPE IF EXISTS "{tenant_schema}"."enum_nomina_ReceptorTipoJornada"')
    op.execute(f'DROP TYPE IF EXISTS "{tenant_schema}"."enum_nomina_ReceptorTipoRegimen"')
    op.execute(f'DROP TYPE IF EXISTS "{tenant_schema}"."enum_nomina_ReceptorRiesgoPuesto"')
    op.execute(f'DROP TYPE IF EXISTS "{tenant_schema}"."enum_nomina_ReceptorPeriodicidadPago"')
    op.execute(f'DROP TYPE IF EXISTS "{tenant_schema}"."enum_nomina_ReceptorClaveEntFed"')
    op.execute(f'DROP TYPE IF EXISTS "{tenant_schema}"."enum_sat_query_download_type"')
    op.execute(f'DROP TYPE IF EXISTS "{tenant_schema}"."enum_sat_query_request_type"')
    op.execute(f'DROP TYPE IF EXISTS "{tenant_schema}"."enum_sat_query_state"')
    op.execute(f'DROP TYPE IF EXISTS "{tenant_schema}"."enum_sat_query_technology"')
    op.execute(f'DROP TYPE IF EXISTS "{tenant_schema}"."enum_nomina_ReceptorBanco"')


def downgrade():
    tenant_schema = resolve_schema("per_tenant")

    # Revert column type changes back to original enum types
    # First recreate the old enum types in the tenant schema
    op.execute(
        f"CREATE TYPE \"{tenant_schema}\".\"enum_add_sync_request_state\" AS ENUM ('DRAFT', 'SENT', 'ERROR')"
    )
    op.execute(
        f"CREATE TYPE \"{tenant_schema}\".\"cfdiexportstate\" AS ENUM ('SENT', 'TO_DOWNLOAD', 'ERROR')"
    )
    op.execute(f"CREATE TYPE \"{tenant_schema}\".\"exportdatatype\" AS ENUM ('CFDI', 'IVA', 'ISR')")
    op.execute(f"CREATE TYPE \"{tenant_schema}\".\"enum_nomina_Version\" AS ENUM ('1.1', '1.2')")
    op.execute(f"CREATE TYPE \"{tenant_schema}\".\"enum_nomina_TipoNomina\" AS ENUM ('O', 'E')")
    op.execute(
        f"CREATE TYPE \"{tenant_schema}\".\"enum_nomina_ReceptorTipoContrato\" AS ENUM ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '99')"
    )
    op.execute(
        f"CREATE TYPE \"{tenant_schema}\".\"enum_nomina_ReceptorTipoJornada\" AS ENUM ('01', '02', '03', '04', '05', '06', '07', '08', '99')"
    )
    op.execute(
        f"CREATE TYPE \"{tenant_schema}\".\"enum_nomina_ReceptorTipoRegimen\" AS ENUM ('02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '99')"
    )
    op.execute(
        f"CREATE TYPE \"{tenant_schema}\".\"enum_nomina_ReceptorRiesgoPuesto\" AS ENUM ('1', '2', '3', '4', '5', '99')"
    )
    op.execute(
        f"CREATE TYPE \"{tenant_schema}\".\"enum_nomina_ReceptorPeriodicidadPago\" AS ENUM ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '99')"
    )
    op.execute(
        f"CREATE TYPE \"{tenant_schema}\".\"enum_nomina_ReceptorBanco\" AS ENUM ('002', '006', '009', '012', '014', '019', '021', '030', '032', '036', '037', '042', '044', '058', '059', '060', '062', '072', '102', '103', '106', '108', '110', '112', '113', '116', '124', '126', '127', '128', '129', '130', '131', '132', '133', '134', '135', '136', '137', '138', '139', '140', '141', '143', '145', '147', '148', '149', '150', '151', '152', '153', '154', '155', '156', '157', '158', '159', '160', '166', '168', '600', '601', '602', '605', '606', '607', '608', '610', '614', '615', '616', '617', '618', '619', '620', '621', '622', '623', '626', '627', '628', '629', '630', '631', '632', '633', '634', '636', '637', '638', '640', '642', '646', '647', '648', '649', '651', '652', '653', '655', '656', '659', '670', '901', '902')"
    )
    op.execute(
        f"CREATE TYPE \"{tenant_schema}\".\"enum_nomina_ReceptorClaveEntFed\" AS ENUM ('AGU', 'BCN', 'BCS', 'CAM', 'CHP', 'CHH', 'COA', 'COL', 'CMX', 'DIF', 'DUR', 'GUA', 'GRO', 'HID', 'JAL', 'MEX', 'MIC', 'MOR', 'NAY', 'NLE', 'OAX', 'PUE', 'QUE', 'ROO', 'SLP', 'SIN', 'SON', 'TAB', 'TAM', 'TLA', 'VER', 'YUC', 'ZAC', 'AL', 'AK', 'AZ', 'AR', 'CA', 'NC', 'SC', 'CO', 'CT', 'ND', 'SD', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NJ', 'NY', 'NH', 'NM', 'OH', 'OK', 'OR', 'PA', 'RI', 'TN', 'TX', 'UT', 'VT', 'VA', 'WV', 'WA', 'WI', 'WY', 'ON', 'QC', 'NS', 'NB', 'MB', 'BC', 'PE', 'SK', 'AB', 'NL', 'NT', 'YT', 'UN')"
    )
    op.execute(
        f"CREATE TYPE \"{tenant_schema}\".\"enum_sat_query_download_type\" AS ENUM ('ISSUED', 'RECEIVED', 'BOTH')"
    )
    op.execute(
        f"CREATE TYPE \"{tenant_schema}\".\"enum_sat_query_request_type\" AS ENUM ('CFDI', 'METADATA', 'BOTH')"
    )
    op.execute(
        f"CREATE TYPE \"{tenant_schema}\".\"enum_sat_query_state\" AS ENUM ('DRAFT', 'SENT', 'TO_DOWNLOAD', 'DOWNLOADED', 'TO_SCRAP', 'DELAYED', 'PROCESSING', 'ERROR_IN_CERTS', 'ERROR_SAT_WS_UNKNOWN', 'ERROR_SAT_WS_INTERNAL', 'ERROR_TOO_BIG', 'TIME_LIMIT_REACHED', 'ERROR', 'SCRAP_FAILED', 'CANT_SCRAP', 'MANUALLY_CANCELLED', 'SPLITTED', 'INFORMATION_NOT_FOUND', 'SUBSTITUTED', 'SUBSTITUTED_TO_SCRAP', 'PROCESSED', 'SCRAPPED')"
    )
    op.execute(
        f"CREATE TYPE \"{tenant_schema}\".\"enum_sat_query_technology\" AS ENUM ('WebService', 'Scraper')"
    )

    # Revert column types to use tenant schema enums
    op.alter_column(
        "sat_query",
        "technology",
        schema=tenant_schema,
        server_default=None,
    )
    op.alter_column(
        "sat_query",
        "technology",
        schema=tenant_schema,
        type_=sa.Enum(name="enum_sat_query_technology", schema=tenant_schema),
        postgresql_using=f'technology::text::"{tenant_schema}"."enum_sat_query_technology"',
        server_default="WebService",
    )

    op.alter_column(
        "sat_query",
        "state",
        schema=tenant_schema,
        type_=sa.Enum(name="enum_sat_query_state", schema=tenant_schema),
        postgresql_using=f'state::text::"{tenant_schema}"."enum_sat_query_state"',
    )
    op.alter_column(
        "sat_query",
        "request_type",
        schema=tenant_schema,
        type_=sa.Enum(name="enum_sat_query_request_type", schema=tenant_schema),
        postgresql_using=f'request_type::text::"{tenant_schema}"."enum_sat_query_request_type"',
    )
    op.alter_column(
        "sat_query",
        "download_type",
        schema=tenant_schema,
        type_=sa.Enum(name="enum_sat_query_download_type", schema=tenant_schema),
        postgresql_using=f'download_type::text::"{tenant_schema}"."enum_sat_query_download_type"',
    )

    # Remove company_identifier columns
    op.drop_column("payment_relation", "company_identifier", schema=tenant_schema)
    op.drop_column("payment", "company_identifier", schema=tenant_schema)

    # Revert nomina column types
    op.alter_column(
        "nomina",
        "ReceptorClaveEntFed",
        schema=tenant_schema,
        type_=sa.Enum(name="enum_nomina_ReceptorClaveEntFed", schema=tenant_schema),
        postgresql_using=f'"ReceptorClaveEntFed"::text::"{tenant_schema}"."enum_nomina_ReceptorClaveEntFed"',
    )
    op.alter_column(
        "nomina",
        "ReceptorPeriodicidadPago",
        schema=tenant_schema,
        type_=sa.Enum(name="enum_nomina_ReceptorPeriodicidadPago", schema=tenant_schema),
        postgresql_using=f'"ReceptorPeriodicidadPago"::text::"{tenant_schema}"."enum_nomina_ReceptorPeriodicidadPago"',
    )
    op.alter_column(
        "nomina",
        "ReceptorRiesgoPuesto",
        schema=tenant_schema,
        type_=sa.Enum(name="enum_nomina_ReceptorRiesgoPuesto", schema=tenant_schema),
        postgresql_using=f'"ReceptorRiesgoPuesto"::text::"{tenant_schema}"."enum_nomina_ReceptorRiesgoPuesto"',
    )
    op.alter_column(
        "nomina",
        "ReceptorTipoRegimen",
        schema=tenant_schema,
        type_=sa.Enum(name="enum_nomina_ReceptorTipoRegimen", schema=tenant_schema),
        postgresql_using=f'"ReceptorTipoRegimen"::text::"{tenant_schema}"."enum_nomina_ReceptorTipoRegimen"',
    )
    op.alter_column(
        "nomina",
        "ReceptorTipoJornada",
        schema=tenant_schema,
        type_=sa.Enum(name="enum_nomina_ReceptorTipoJornada", schema=tenant_schema),
        postgresql_using=f'"ReceptorTipoJornada"::text::"{tenant_schema}"."enum_nomina_ReceptorTipoJornada"',
    )
    op.alter_column(
        "nomina",
        "ReceptorTipoContrato",
        schema=tenant_schema,
        type_=sa.Enum(name="enum_nomina_ReceptorTipoContrato", schema=tenant_schema),
        postgresql_using=f'"ReceptorTipoContrato"::text::"{tenant_schema}"."enum_nomina_ReceptorTipoContrato"',
    )
    op.alter_column(
        "nomina",
        "TipoNomina",
        schema=tenant_schema,
        type_=sa.Enum(name="enum_nomina_TipoNomina", schema=tenant_schema),
        postgresql_using=f'"TipoNomina"::text::"{tenant_schema}"."enum_nomina_TipoNomina"',
    )
    op.alter_column(
        "nomina",
        "Version",
        schema=tenant_schema,
        type_=sa.Enum(name="enum_nomina_Version", schema=tenant_schema),
        postgresql_using=f'"Version"::text::"{tenant_schema}"."enum_nomina_Version"',
    )

    op.drop_column("nomina", "company_identifier", schema=tenant_schema)
    op.drop_column("cfdi_relation", "company_identifier", schema=tenant_schema)

    # Revert cfdi_export column types
    op.alter_column(
        "cfdi_export",
        "export_data_type",
        schema=tenant_schema,
        server_default=None,
    )
    op.alter_column(
        "cfdi_export",
        "export_data_type",
        schema=tenant_schema,
        type_=sa.Enum(name="exportdatatype", schema=tenant_schema),
        postgresql_using=f'export_data_type::text::"{tenant_schema}"."exportdatatype"',
        server_default="CFDI",
    )
    op.alter_column(
        "cfdi_export",
        "state",
        schema=tenant_schema,
        type_=sa.Enum(name="cfdiexportstate", schema=tenant_schema),
        postgresql_using=f'state::text::"{tenant_schema}"."cfdiexportstate"',
    )

    op.drop_column("cfdi", "company_identifier", schema=tenant_schema)

    op.alter_column(
        "add_sync_request",
        "state",
        schema=tenant_schema,
        type_=sa.Enum(name="enum_add_sync_request_state", schema=tenant_schema),
        postgresql_using=f'state::text::"{tenant_schema}"."enum_add_sync_request_state"',
    )
