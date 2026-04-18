exclusive_fields_per_section = {
    # Trasladado
    "Trasladado": [],
    "Trasladado - Facturas de contado": [],
    "Trasladado - Cobro Facturas de crédito": [
        ("pr_count", "TipoDeComprobante"),
    ],
    "Trasladado - No considerados IVA": [],
    "Trasladado - Periodo IVA Reasignado": [],
    # Acreditable
    "Acreditable": [],
    "Acreditable - Facturas de contado": [],
    "Acreditable - Pago Facturas de crédito": [
        ("pr_count", "TipoDeComprobante"),
    ],
    "Acreditable - No considerados IVA": [],
    "Acreditable - Periodo IVA Reasignado": [],
}


def include_fields_in_section(section, default_fields, fields_per_section):
    section_fields = fields_per_section.get(section, [])
    if section_fields:
        for field in section_fields:
            field_to_insert, previous_field = field
            previous_field_index = default_fields.index(previous_field)
            default_fields.insert(previous_field_index + 1, field_to_insert)
