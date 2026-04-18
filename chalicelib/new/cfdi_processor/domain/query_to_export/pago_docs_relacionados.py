from sqlalchemy import (
    DateTime,
    Numeric,
    String,
    case,
    cast,
    func,
    select,
    text,
    true,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Query, Session, aliased
from sqlalchemy.sql import Select, lateral

from chalicelib.controllers import Domain, get_filters
from chalicelib.schema.models.tenant.cfdi import CFDI

pagos_column_types = {
    "Fecha de pago": DateTime,
    "Fecha de emisión": DateTime,
    "UUID": UUID,
    "Serie": String,
    "Folio": String,
    "RFC emisor": String,
    "Emisor": String,
    "RFC receptor": String,
    "Receptor": String,
    "Forma de pago": String,
    "Moneda de pago": String,
    "DR - Fecha de emisión": DateTime,
    "DR - Serie": String,
    "DR - Folio": String,
    "DR - UUID": UUID,
    "DR - Uso de CFDI": String,
    "DR - Objeto de impuesto": String,
    "DR - Moneda": String,
    "DR - Equivalencia": Numeric,
    "DR - Numero de parcialidad": Numeric,
    "DR - Importe pagado": Numeric,
    "DR - Importe pagado MXN": Numeric,
    "DR - Base IVA 16 %": Numeric,
    "DR - Base IVA 8 %": Numeric,
    "DR - Base IVA 0 %": Numeric,
    "DR - Base IVA Exento": Numeric,
    "DR - IVA 16 %": Numeric,
    "DR - IVA 8 %": Numeric,
    "DR - IVA total": Numeric,
    "DR - Base IEPS": Numeric,
    "DR - Factor IEPS": String,
    "DR - Tasa o cuota IEPS": Numeric,
    "DR - IEPS": Numeric,
    "DR - Retenciónes ISR": Numeric,
    "DR - Retenciónes IVA": Numeric,
}


def _pago_docs_core_select(session: Session, domain: Domain) -> Select:
    cfdi = aliased(CFDI)  # CFDI de pago
    cfdi2 = CFDI.__table__.alias("cfdi2")  # CFDI emitido (del DR)
    filters = get_filters(cfdi, domain, session)

    doctos = lateral(
        text("""
                SELECT
                  (xpath('string(//*[local-name()="Pago"]/@FechaPago)',
                  cfdi_1.xml_content))[1]::text AS "FechaPago",
                  (xpath('string(//*[local-name()="Pago"]/@FormaDePagoP)',
                   cfdi_1.xml_content))[1]::text AS "FormaDePagoP",
                  (xpath('string(//*[local-name()="Pago"]/@MonedaP)',
                   cfdi_1.xml_content))[1]::text AS "MonedaP",
                  NULLIF((xpath('string(//*[local-name()="Pago"]/@TipoCambioP)',
                   cfdi_1.xml_content))[1]::text,'')::numeric AS "TipoCambioP",
                  (xpath('string(//@IdDocumento)',
                   xmlparse(content d::text)))[1]::text AS "IdDocumento",
                  NULLIF((xpath('string(//@Serie)',
                   xmlparse(content d::text)))[1]::text,'') AS "DR_Serie",
                  NULLIF((xpath('string(//@Folio)',
                   xmlparse(content d::text)))[1]::text,'') AS "DR_Folio",
                  NULLIF((xpath('string(//@MonedaDR)',
                   xmlparse(content d::text)))[1]::text,'') AS "MonedaDR",
                  NULLIF((xpath('string(//@EquivalenciaDR)',
                  xmlparse(content d::text)))[1]::text,'')::numeric AS "EquivalenciaDR",
                  NULLIF((xpath('string(//@NumParcialidad)',
                  xmlparse(content d::text)))[1]::text,'')::int     AS "NumParcialidad",
                  NULLIF((xpath('string(//@ImpPagado)',
                   xmlparse(content d::text)))[1]::text,'')::numeric AS "ImpPagado",
                  (xpath('string(//@ObjetoImpDR)',
                   xmlparse(content d::text)))[1]::text AS "ObjetoImpDR",

                  CASE
                    WHEN (xpath('string(//@MonedaDR)',
                     xmlparse(content d::text)))[1]::text =
                         (xpath('string(//*[local-name()="Pago"]/@MonedaP)',
                         cfdi_1.xml_content))[1]::text
                    THEN NULLIF((xpath('string(//@ImpPagado)',
                    xmlparse(content d::text)))[1]::text,'')::numeric
                    ELSE NULLIF((xpath('string(//@ImpPagado)',
                     xmlparse(content d::text)))[1]::text,'')::numeric
                         / NULLIF((xpath('string(//@EquivalenciaDR)',
                          xmlparse(content d::text)))[1]::text,'')::numeric
                  END AS "ImpPagadoMP",

                  CASE
                    WHEN (xpath('string(//*[local-name()="Pago"]/@MonedaP)',
                     cfdi_1.xml_content))[1]::text = 'MXN'
                         OR NULLIF((xpath('string(//*[local-name()="Pago"]/@TipoCambioP)',
                          cfdi_1.xml_content))[1]::text,'')::numeric IS NULL
                         OR NULLIF((xpath('string(//*[local-name()="Pago"]/@TipoCambioP)',
                          cfdi_1.xml_content))[1]::text,'')::numeric = 0
                    THEN
                        CASE
                          WHEN (xpath('string(//@MonedaDR)',
                          xmlparse(content d::text)))[1]::text =
                               (xpath('string(//*[local-name()="Pago"]/@MonedaP)',
                                cfdi_1.xml_content))[1]::text
                          THEN NULLIF((xpath('string(//@ImpPagado)',
                           xmlparse(content d::text)))[1]::text,'')::numeric
                          ELSE NULLIF((xpath('string(//@ImpPagado)',
                          xmlparse(content d::text)))[1]::text,'')::numeric
                               / NULLIF((xpath('string(//@EquivalenciaDR)',
                               xmlparse(content d::text)))[1]::text,'')::numeric
                        END
                    ELSE
                        (CASE
                          WHEN (xpath('string(//@MonedaDR)',
                           xmlparse(content d::text)))[1]::text =
                               (xpath('string(//*[local-name()="Pago"]/@MonedaP)',
                                cfdi_1.xml_content))[1]::text
                          THEN NULLIF((xpath('string(//@ImpPagado)',
                           xmlparse(content d::text)))[1]::text,'')::numeric
                          ELSE NULLIF((xpath('string(//@ImpPagado)',
                           xmlparse(content d::text)))[1]::text,'')::numeric
                               / NULLIF((xpath('string(//@EquivalenciaDR)',
                                xmlparse(content d::text)))[1]::text,'')::numeric
                        END) * NULLIF((xpath('string(//*[local-name()="Pago"]/@TipoCambioP)',
                         cfdi_1.xml_content))[1]::text,'')::numeric
                  END AS "ImpPagadoMXN",

                  d AS docto_node
                FROM unnest(xpath(
                  '//*[local-name()="Pago"]//*[local-name()="DoctoRelacionado"]',
                  cfdi_1.xml_content)) AS d
            """).columns(
            FechaPago=String,
            FormaDePagoP=String,
            MonedaP=String,
            TipoCambioP=Numeric,
            IdDocumento=String,
            DR_Serie=String,
            DR_Folio=String,
            MonedaDR=String,
            EquivalenciaDR=Numeric,
            NumParcialidad=Numeric,
            ImpPagado=Numeric,
            ObjetoImpDR=String,
            ImpPagadoMP=Numeric,
            ImpPagadoMXN=Numeric,
            docto_node=String,
        )
    ).alias("doctos")

    traslados = lateral(
        text("""
                SELECT
                  SUM(CASE WHEN tasa = 0.16 THEN base_mxn ELSE 0 END) AS "BaseIVAMXN16",
                  SUM(CASE WHEN tasa = 0.08 THEN base_mxn ELSE 0 END) AS "BaseIVAMXN08",
                  SUM(CASE WHEN tasa = 0.00 THEN base_mxn ELSE 0 END) AS "BaseIVAMXN00",
                  SUM(CASE WHEN tipofactor = 'Exento' THEN base_mxn ELSE 0 END) AS "BaseIVAMXNEx",
                  SUM(CASE WHEN tasa = 0.16 THEN imp_mxn ELSE 0 END) AS "IVAMXN16",
                  SUM(CASE WHEN tasa = 0.08 THEN imp_mxn ELSE 0 END) AS "IVAMXN08",

                  SUM(CASE WHEN tasa = 0.16 THEN base_mp ELSE 0 END) AS "BaseIVAMP16",
                  SUM(CASE WHEN tasa = 0.08 THEN base_mp ELSE 0 END) AS "BaseIVAMP08",
                  SUM(CASE WHEN tasa = 0.00 THEN base_mp ELSE 0 END) AS "BaseIVAMP00",
                  SUM(CASE WHEN tipofactor = 'Exento' THEN base_mp ELSE 0 END) AS "BaseIVAMPEx",
                  SUM(CASE WHEN tasa = 0.16 THEN imp_mp ELSE 0 END) AS "IVAMP16",
                  SUM(CASE WHEN tasa = 0.08 THEN imp_mp ELSE 0 END) AS "IVAMP08"
                FROM (
                  SELECT
                    NULLIF((xpath('string(//@BaseDR)',
                     xmlparse(content t::text)))[1]::text, '')::numeric AS base_orig,
                    NULLIF((xpath('string(//@ImporteDR)',
                     xmlparse(content t::text)))[1]::text, '')::numeric AS imp_orig,
                    NULLIF((xpath('string(//@TasaOCuotaDR)',
                    xmlparse(content t::text)))[1]::text, '')::numeric AS tasa,
                    (xpath('string(//@TipoFactorDR)',
                     xmlparse(content t::text)))[1]::text AS tipofactor,

                    CASE
                      WHEN doctos."MonedaDR" = doctos."MonedaP"
                      THEN NULLIF((xpath('string(//@BaseDR)',
                       xmlparse(content t::text)))[1]::text, '')::numeric
                      ELSE NULLIF((xpath('string(//@BaseDR)',
                      xmlparse(content t::text)))[1]::text, '')::numeric
                           / COALESCE(NULLIF(doctos."EquivalenciaDR", 0), 1)
                    END AS base_mp,

                    CASE
                      WHEN doctos."MonedaDR" = doctos."MonedaP"
                      THEN NULLIF((xpath('string(//@ImporteDR)',
                       xmlparse(content t::text)))[1]::text, '')::numeric
                      ELSE NULLIF((xpath('string(//@ImporteDR)',
                       xmlparse(content t::text)))[1]::text, '')::numeric
                           / COALESCE(NULLIF(doctos."EquivalenciaDR", 0), 1)
                    END AS imp_mp,

                    CASE
                      WHEN doctos."MonedaP" = 'MXN'
                       OR doctos."TipoCambioP" IS NULL OR doctos."TipoCambioP" = 0
                      THEN
                        CASE
                          WHEN doctos."MonedaDR" = doctos."MonedaP"
                          THEN NULLIF((xpath('string(//@BaseDR)',
                          xmlparse(content t::text)))[1]::text, '')::numeric
                          ELSE NULLIF((xpath('string(//@BaseDR)',
                           xmlparse(content t::text)))[1]::text, '')::numeric
                               / COALESCE(NULLIF(doctos."EquivalenciaDR", 0), 1)
                        END
                      ELSE
                        (
                          CASE
                            WHEN doctos."MonedaDR" = doctos."MonedaP"
                            THEN NULLIF((xpath('string(//@BaseDR)',
                             xmlparse(content t::text)))[1]::text, '')::numeric
                            ELSE NULLIF((xpath('string(//@BaseDR)',
                             xmlparse(content t::text)))[1]::text, '')::numeric
                                 / COALESCE(NULLIF(doctos."EquivalenciaDR", 0), 1)
                          END
                        ) * doctos."TipoCambioP"
                    END AS base_mxn,

                    CASE
                      WHEN doctos."MonedaP" = 'MXN'
                       OR doctos."TipoCambioP" IS NULL OR doctos."TipoCambioP" = 0
                      THEN
                        CASE
                          WHEN doctos."MonedaDR" = doctos."MonedaP"
                          THEN NULLIF((xpath('string(//@ImporteDR)',
                          xmlparse(content t::text)))[1]::text, '')::numeric
                          ELSE NULLIF((xpath('string(//@ImporteDR)',
                           xmlparse(content t::text)))[1]::text, '')::numeric
                               / COALESCE(NULLIF(doctos."EquivalenciaDR", 0), 1)
                        END
                      ELSE
                        (
                          CASE
                            WHEN doctos."MonedaDR" = doctos."MonedaP"
                            THEN NULLIF((xpath('string(//@ImporteDR)',
                             xmlparse(content t::text)))[1]::text, '')::numeric
                            ELSE NULLIF((xpath('string(//@ImporteDR)',
                             xmlparse(content t::text)))[1]::text, '')::numeric
                                 / COALESCE(NULLIF(doctos."EquivalenciaDR", 0), 1)
                          END
                        ) * doctos."TipoCambioP"
                    END AS imp_mxn
                  FROM unnest(xpath(
                    './/*[local-name()="TrasladoDR"][@ImpuestoDR="002"]',
                    doctos.docto_node)) AS t
                ) sub
            """).columns(
            BaseIVAMXN16=Numeric,
            BaseIVAMXN08=Numeric,
            BaseIVAMXN00=Numeric,
            BaseIVAMXNEx=Numeric,
            IVAMXN16=Numeric,
            IVAMXN08=Numeric,
            BaseIVAMP16=Numeric,
            BaseIVAMP08=Numeric,
            BaseIVAMP00=Numeric,
            BaseIVAMPEx=Numeric,
            IVAMP16=Numeric,
            IVAMP08=Numeric,
        )
    ).alias("traslados")

    #  RETIVA
    retiva = lateral(
        text("""
                SELECT
                  SUM(
                    CASE
                      WHEN doctos."MonedaDR" = doctos."MonedaP"
                      THEN COALESCE(NULLIF((xpath('string(//@ImporteDR)',
                       xmlparse(content r::text)))[1]::text,'')::numeric,0)
                      ELSE COALESCE(NULLIF((xpath('string(//@ImporteDR)',
                       xmlparse(content r::text)))[1]::text,'')::numeric,0)
                           / COALESCE(NULLIF(doctos."EquivalenciaDR",0),1)
                    END
                  ) AS "RetIVA_MP",

                  SUM(
                    CASE
                      WHEN doctos."MonedaP" = 'MXN' OR
                       doctos."TipoCambioP" IS NULL OR doctos."TipoCambioP" = 0
                      THEN
                        CASE
                          WHEN doctos."MonedaDR" = doctos."MonedaP"
                          THEN COALESCE(NULLIF((xpath('string(//@ImporteDR)',
                           xmlparse(content r::text)))[1]::text,'')::numeric,0)
                          ELSE COALESCE(NULLIF((xpath('string(//@ImporteDR)',
                           xmlparse(content r::text)))[1]::text,'')::numeric,0)
                               / COALESCE(NULLIF(doctos."EquivalenciaDR",0),1)
                        END
                      ELSE
                        (CASE
                          WHEN doctos."MonedaDR" = doctos."MonedaP"
                          THEN COALESCE(NULLIF((xpath('string(//@ImporteDR)',
                           xmlparse(content r::text)))[1]::text,'')::numeric,0)
                          ELSE COALESCE(NULLIF((xpath('string(//@ImporteDR)',
                           xmlparse(content r::text)))[1]::text,'')::numeric,0)
                               / COALESCE(NULLIF(doctos."EquivalenciaDR",0),1)
                        END) * doctos."TipoCambioP"
                    END
                  ) AS "RetIVA_MXN"
                FROM unnest(xpath(
                    './/*[local-name()="RetencionDR"][@ImpuestoDR="002"]',
                    doctos.docto_node)) AS r
            """).columns(RetIVA_MP=Numeric, RetIVA_MXN=Numeric)
    ).alias("retiva")

    ieps = lateral(
        text("""
                SELECT
                  -- Keep factor and rate as-is (non-summed)
                  MAX((xpath('string(//@TipoFactorDR)',
                  xmlparse(content t::text)))[1]::text) AS "TipoFactorDR",
                  MAX(NULLIF((xpath('string(//@TasaOCuotaDR)',
                   xmlparse(content t::text)))[1]::text,'')::numeric) AS "TasaOCuotaDR",

                  -- Convert base to MXN
                  SUM(
                    CASE
                      WHEN doctos."MonedaP" = 'MXN'
                       OR doctos."TipoCambioP" IS NULL OR doctos."TipoCambioP" = 0
                      THEN
                        CASE
                          WHEN doctos."MonedaDR" = doctos."MonedaP"
                          THEN NULLIF((xpath('string(//@BaseDR)',
                           xmlparse(content t::text)))[1]::text,'')::numeric
                          ELSE NULLIF((xpath('string(//@BaseDR)',
                           xmlparse(content t::text)))[1]::text,'')::numeric
                               / COALESCE(NULLIF(doctos."EquivalenciaDR", 0), 1)
                        END
                      ELSE
                        (
                          CASE
                            WHEN doctos."MonedaDR" = doctos."MonedaP"
                            THEN NULLIF((xpath('string(//@BaseDR)',
                             xmlparse(content t::text)))[1]::text,'')::numeric
                            ELSE NULLIF((xpath('string(//@BaseDR)',
                             xmlparse(content t::text)))[1]::text,'')::numeric
                                 / COALESCE(NULLIF(doctos."EquivalenciaDR", 0), 1)
                          END
                        ) * doctos."TipoCambioP"
                    END
                  ) AS "BaseIEPSMXN",

                  -- Convert IEPS amount to MXN
                  SUM(
                    CASE
                      WHEN doctos."MonedaP" = 'MXN'
                       OR doctos."TipoCambioP" IS NULL OR doctos."TipoCambioP" = 0
                      THEN
                        CASE
                          WHEN doctos."MonedaDR" = doctos."MonedaP"
                          THEN NULLIF((xpath('string(//@ImporteDR)',
                           xmlparse(content t::text)))[1]::text,'')::numeric
                          ELSE NULLIF((xpath('string(//@ImporteDR)',
                           xmlparse(content t::text)))[1]::text,'')::numeric
                               / COALESCE(NULLIF(doctos."EquivalenciaDR", 0), 1)
                        END
                      ELSE
                        (
                          CASE
                            WHEN doctos."MonedaDR" = doctos."MonedaP"
                            THEN NULLIF((xpath('string(//@ImporteDR)',
                             xmlparse(content t::text)))[1]::text,'')::numeric
                            ELSE NULLIF((xpath('string(//@ImporteDR)',
                             xmlparse(content t::text)))[1]::text,'')::numeric
                                 / COALESCE(NULLIF(doctos."EquivalenciaDR", 0), 1)
                          END
                        ) * doctos."TipoCambioP"
                    END
                  ) AS "IEPSMXN"
                FROM unnest(xpath(
                    './/*[local-name()="TrasladoDR"][@ImpuestoDR="003"]',
                    doctos.docto_node)) AS t
            """).columns(
            TipoFactorDR=String,
            TasaOCuotaDR=Numeric,
            BaseIEPSMXN=Numeric,
            IEPSMXN=Numeric,
        )
    ).alias("ieps")

    isr = lateral(
        text("""
                SELECT
                  SUM(
                    CASE
                      WHEN doctos."MonedaP" = 'MXN' OR
                       doctos."TipoCambioP" IS NULL OR doctos."TipoCambioP" = 0
                      THEN
                        CASE
                          WHEN doctos."MonedaDR" = doctos."MonedaP"
                          THEN COALESCE(NULLIF((xpath('string(//@ImporteDR)',
                           xmlparse(content r::text)))[1]::text,'')::numeric,0)
                          ELSE COALESCE(NULLIF((xpath('string(//@ImporteDR)',
                           xmlparse(content r::text)))[1]::text,'')::numeric,0)
                               / COALESCE(NULLIF(doctos."EquivalenciaDR",0),1)
                        END
                      ELSE
                        (CASE
                          WHEN doctos."MonedaDR" = doctos."MonedaP"
                          THEN COALESCE(NULLIF((xpath('string(//@ImporteDR)',
                           xmlparse(content r::text)))[1]::text,'')::numeric,0)
                          ELSE COALESCE(NULLIF((xpath('string(//@ImporteDR)',
                           xmlparse(content r::text)))[1]::text,'')::numeric,0)
                               / COALESCE(NULLIF(doctos."EquivalenciaDR",0),1)
                        END) * doctos."TipoCambioP"
                    END
                  ) AS "RetISR_MXN"
                FROM unnest(xpath(
                    './/*[local-name()="RetencionDR"][@ImpuestoDR="001"]',
                    doctos.docto_node)) AS r
            """).columns(RetISR_MXN=Numeric)
    ).alias("isr")

    is_issued_value = next((value for field, _cmp, value in domain if field == "is_issued"), True)
    if not isinstance(is_issued_value, bool):  # minimal fallback
        is_issued_value = str(is_issued_value).lower() in {"true"}

    party_rfc_label = "RFC receptor" if is_issued_value else "RFC emisor"
    party_name_label = "Receptor" if is_issued_value else "Emisor"

    stmt = (
        select(
            doctos.c.FechaPago.label("Fecha de pago"),
            cfdi.Fecha.label("Fecha de emisión"),
            cfdi.UUID,
            cfdi.Serie,
            cfdi.Folio,
            case(
                [(cfdi.is_issued == true(), cfdi.RfcReceptor)],
                else_=cfdi.RfcEmisor,
            ).label(party_rfc_label),
            case(
                [(cfdi.is_issued == true(), cfdi.NombreReceptor)],
                else_=cfdi.NombreEmisor,
            ).label(party_name_label),
            doctos.c.FormaDePagoP.label("Forma de pago"),
            doctos.c.MonedaP.label("Moneda de pago"),
            cfdi2.c.Fecha.label("DR - Fecha de emisión"),
            doctos.c.DR_Serie.label("DR - Serie"),
            doctos.c.DR_Folio.label("DR - Folio"),
            doctos.c.IdDocumento.label("DR - UUID"),
            cfdi2.c.UsoCFDIReceptor.label("DR - Uso de CFDI"),
            doctos.c.ObjetoImpDR.label("DR - Objeto de impuesto"),
            doctos.c.MonedaDR.label("DR - Moneda"),
            func.trunc(doctos.c.EquivalenciaDR, 2).label("DR - Equivalencia"),
            func.trunc(doctos.c.NumParcialidad, 2).label("DR - Numero de parcialidad"),
            func.trunc(doctos.c.ImpPagadoMP, 2).label("DR - Importe pagado"),
            func.trunc(doctos.c.ImpPagadoMXN, 2).label("DR - Importe pagado MXN"),
            func.trunc(traslados.c.BaseIVAMXN16, 2).label("DR - Base IVA 16 %"),
            func.trunc(traslados.c.BaseIVAMXN08, 2).label("DR - Base IVA 8 %"),
            func.trunc(traslados.c.BaseIVAMXN00, 2).label("DR - Base IVA 0 %"),
            func.trunc(traslados.c.BaseIVAMXNEx, 2).label("DR - Base IVA Exento"),
            func.trunc(traslados.c.IVAMXN16, 2).label("DR - IVA 16 %"),
            func.trunc(traslados.c.IVAMXN08, 2).label("DR - IVA 8 %"),
            func.trunc(traslados.c.IVAMXN16 + traslados.c.IVAMXN08, 2).label("DR - IVA total"),
            func.trunc(ieps.c.BaseIEPSMXN, 2).label("DR - Base IEPS"),
            ieps.c.TipoFactorDR.label("DR - Factor IEPS"),
            func.trunc(ieps.c.TasaOCuotaDR, 2).label("DR - Tasa o cuota IEPS"),
            func.trunc(ieps.c.IEPSMXN, 2).label("DR - IEPS"),
            func.trunc(isr.c.RetISR_MXN, 2).label("DR - Retenciónes ISR"),
            func.trunc(retiva.c.RetIVA_MXN, 2).label("DR - Retenciónes IVA"),
        )
        .select_from(cfdi)
        .join(doctos, true(), isouter=True)
        .join(cfdi2, func.lower(doctos.c.IdDocumento) == cast(cfdi2.c.UUID, String), isouter=True)
        .join(traslados, true(), isouter=True)
        .join(retiva, true(), isouter=True)
        .join(ieps, true(), isouter=True)
        .join(isr, true(), isouter=True)
        .where(*filters)
        .order_by(doctos.c.FechaPago.desc())
    )

    return stmt


def query_pago_docs_relacionados(session: Session, domain: Domain) -> Query:
    """
    Devuelve un Query que selecciona **únicamente** las columnas calculadas
    en `_pago_docs_core_select()`, pero sigue permitiendo filtrar por CFDI.
    """
    core_stmt = _pago_docs_core_select(session, domain)
    sub = core_stmt.subquery("pago_docs")
    PagoDocs = aliased(sub)

    # --- sólo las columnas del sub-select ---
    cols = list(PagoDocs.c)

    q = session.query(*cols).select_from(CFDI).outerjoin(PagoDocs, PagoDocs.c.UUID == CFDI.UUID)
    return q
