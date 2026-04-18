#
# import json
#
# import pytest
# from chalice.test import Client
# from sqlalchemy.orm import Session
#
# from chalicelib.new.isr import query_gastos_nomina_gravada
#
# TOKEN="eyJraWQiOiI3XC9neERXMFJXNUNKMlwvNGZKbVNLZWhER2ZUTW1KajFURUU3WG0wUFBlVVU9IiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI2NDU4MzQwOC04MDExLTcwZGEtMzczZC0yOTc1Zjk3NWJmYTIiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiaXNzIjoiaHR0cHM6XC9cL2NvZ25pdG8taWRwLnVzLWVhc3QtMS5hbWF6b25hd3MuY29tXC91cy1lYXN0LTFfekF3a2VtdlBqIiwiY29nbml0bzp1c2VybmFtZSI6InRyZWszMTNuMkBtb3ptYWlsLmNvbSIsIm9yaWdpbl9qdGkiOiI5YmNhNjVkMy03MjdlLTQ0M2UtYjhiNy03MzNjNzI3ZmE5ODEiLCJhdWQiOiI0Z3VtcGRhMmpiaXRha2cwaHRiMWJkN2VmaiIsImV2ZW50X2lkIjoiOWM3MmY3MDAtMTQzMy00MmM0LWEzZjItMTI4Y2E1Nzg4OWRhIiwidG9rZW5fdXNlIjoiaWQiLCJhdXRoX3RpbWUiOjE3NTY5NDI2ODMsImV4cCI6MTc1Njk0NjI4MywiaWF0IjoxNzU2OTQyNjgzLCJqdGkiOiI4MjlhYzE1MS00YmQwLTRkYmMtOGY1Ni1mNDMwMThkZDQzOTgiLCJlbWFpbCI6InRyZWszMTNuMkBtb3ptYWlsLmNvbSJ9.zLDvJ0EcZX1awNwHY2UOGIvbugcb7fFcZar3aLYDRGKgDcDs4UEu2ytmcchJUWd2hk7V51wWbixzYfNy3Ydj8IUaNE-1_MnF_6qyFdMZhPYBoEloJJqb4dBxbJ8mPeEv6jV36aVcQC2TwQ0xSHgtw66ptYlScR37tbiBP-WDBeF8ieZQj_rBr6hXKne614Sq1_TZGdRi0ZGsuw7kGVaGPgWnTBTrJhpdiEcXzpLxlCvq3ohyMB4EZs3WWbojMUKTK9DaKPR5GxztkJgiF6gGkc0tQXmXlQqc4HJaryFNcbn0b0B3Bez8J5sbS0aYJIBqoq1wr4u523soKXJidRNXmw"
#
# def test_total_deducciones(client: Client):
#     """
#     Query utilizada para validar los resultados:
#         select
#             count(*) as "ConteoCFDIs",
#             SUM("SubTotal") as "SubTotal",
#             SUM("DescuentoMXN") as "DescuentoMXN",
#             SUM("NetoMXN") as "NetoMXN",
#             SUM("RetencionesISRMXN") as "RetencionesISRMXN"
#         from cfdi
#         where
#             "TipoDeComprobante" = 'I'
#             and is_issued = false
#             and "Estatus"
#             and "MetodoPago" = 'PUE'
#             and "FormaPago" in ('02', '03', '04', '05', '06', '28', '29')
#             and "UsoCFDIReceptor" in ('G03', 'G01')
#             and "ExcludeFromISR" = false
#             and "FechaFiltro" BETWEEN '2025-01-21 00:00:00' and '2025-12-31 23:59:59';
#
#     Results:
#         ConteoCFDIs | SubTotal   | DescuentoMXN | NetoMXN     | RetencionesISRMXN
#         212           | 8896475.38 | 0.01        | 8904655.57065  | 34.76
#         1 row selected.
#
#     """
#
#     request_body = {
#         "domain": [
#             ["company_identifier", "=", "745c57aa-8b20-46b8-82c5-3d88bf661ef5"],
#             ["FechaFiltro", ">=", "2025-01-01T00:00:00.000"],
#             ["FechaFiltro", "<", "2025-12-31T00:00:00.000"],
#             ["Estatus", "=", True],
#             ["MetodoPago", "=", "PUE"],
#             ["is_issued", "=", False],
#             ["TipoDeComprobante", "=", "I"],
#             ["FormaPago", "in", ["02", "03", "04", "05", "06", "28", "29"]],
#             ["UsoCFDIReceptor", "in", ["G03", "G01"]],
#             ["ExcludeFromISR", "=", False],
#         ],
#         "fields": ["SubTotal", "DescuentoMXN", "NetoMXN", "RetencionesISRMXN"],
#     }
#
#     headers = {
#         "Content-Type": "application/json",
#         "access_token": TOKEN
#     }
#
#     response = client.http.post(
#         "/CFDI/total_deducciones_cfdi",
#         body=json.dumps(request_body),
#         headers=headers
#     )
#
#     assert response.status_code == 200, f"Status code inesperado: {response.status_code}"
#
#
#     valores_esperados = {
#         "ConteoCFDIs": 212,
#         "SubTotal": 8896475.38,
#         "DescuentoMXN": 0.01,
#         "NetoMXN": 8904655.57065,
#         "RetencionesISRMXN": 34.76
#     }
#
#     # Verificar cada valor específico
#     for key, expected_value in valores_esperados.items():
#         assert key in response.json_body, f"Falta la clave {key} en la respuesta"
#         actual_value = response.json_body[key]
#         assert actual_value == expected_value, f"Valor inesperado para {key}: esperado {expected_value}, obtenido {actual_value}"
#
#
# def test_total_deduciones_pagos(client: Client):
#     """
#     SELECT
#         SUM(pago."pr_count") AS "Relacionados",
#         SUM(pr."BaseIVA16") AS "Base IVA 16%",
#         SUM(pr."BaseIVA8") AS "Base IVA 8%",
#         SUM(pr."BaseIVA0") AS "Base IVA 0%",
#         SUM(pr."BaseIVAExento") AS "Base IVA exento",
#         SUM(
#             COALESCE(pr."BaseIVA16", 0) + COALESCE(pr."BaseIVA8", 0)
#         ) AS "Neto",
#         SUM(COALESCE(retenciones."RetencionesISR", 0)) AS "Retenciones ISR"
#     FROM cfdi i
#         INNER JOIN payment_relation pr ON i."UUID" = pr."UUID_related"
#         INNER JOIN payment p ON pr."UUID" = p."uuid_origin"
#         INNER JOIN cfdi pago ON p."uuid_origin" = pago."UUID"
#         LEFT JOIN LATERAL (
#         SELECT
#             SUM(CAST(elem->>'@ImporteDR' AS numeric)) AS "RetencionesISR"
#         FROM jsonb_array_elements(pr."RetencionesDR") AS elem
#         WHERE elem->>'@ImpuestoDR' = '001'
#         ) AS retenciones ON TRUE
#     WHERE
#     pago."TipoDeComprobante" = 'P'
#     AND p."FormaDePagoP" IN ('02', '03', '04', '05', '06', '28', '29')
#     AND i."UsoCFDIReceptor" IN ('G03', 'G01')
#     AND pago."ExcludeFromISR" = FALSE
#     AND pago."Estatus" = TRUE
#     AND pago.is_issued = FALSE;
#     """
#     request_body = {
#         "domain": [
#             ["company_identifier", "=", "745c57aa-8b20-46b8-82c5-3d88bf661ef5"],
#             ["FechaPago", ">=", "2024-01-01T00:00:00.000"],
#             ["FechaPago", "<", "2025-01-01T00:00:00.000"],
#             ["cfdi_origin.TipoDeComprobante", "=", "P"],
#             ["payment_related.FormaDePagoP", "in", ["02", "03", "04", "05", "06", "28", "29"]],
#             ["cfdi_related.UsoCFDIReceptor", "in", ["G01", "G03"]],
#             ["cfdi_origin.ExcludeFromISR", "=", False],
#             ["cfdi_origin.Estatus", "=", True],
#             ["cfdi_origin.is_issued", "=", False],
#         ],
#         "fields": [
#             "BaseIVA16",
#             "BaseIVA8",
#             "BaseIVA0",
#             "BaseIVAExento",
#             "Neto",
#             "RetencionesISRMXN"
#         ]
#     }
#     headers = {
#         "Content-Type": "application/json",
#         "access_token": TOKEN
#     }
#     response = client.http.post(
#         "/CFDI/total_deducciones_pagos",
#         body=json.dumps(request_body),
#         headers=headers
#     )
#     assert response.status_code == 200, f"Status code inesperado: {response.status_code}"
#
# def test_gran_total(client: Client):
#
#     request_body = {
#         "domain": [
#                 ["company_identifier", "=", "013656c9-6f38-44c2-9ed4-f27007c22061"],
#                 ["start", ">=", "2040-01-01T00:00:00.000"],
#                 ["end", "<", "2045-01-01T00:00:00.000"],
#
#             ]
#     }
#
#     response = client.http.post(
#         "/CFDI/totales",
#         body=json.dumps(request_body),
#         headers={
#             "Content-Type": "application/json",
#             "access_token": TOKEN
#         }
#     )
#     assert response.status_code == 200, f"Status code inesperado: {response.status_code}"
#
#
# @pytest.mark.parametrize("domain", [
#     [
#         ["company_identifier", "=", "013656c9-6f38-44c2-9ed4-f27007c22061"],
#         ["start", ">=", "2020-01-01 00:00:00"],
#         ["end", "<", "2025-01-01 23:59:59"]
#     ]
# ])
# def test_query_gastos_nomina_gravada_runs(session: Session, domain):
#     query = query_gastos_nomina_gravada(session, domain)
#     sql = str(query.statement.compile(compile_kwargs={"literal_binds": True}))
#
#     # Verificamos que compile bien y tenga columnas esperadas
#     assert "count" in sql
#     assert "PercepcionesTotalGravado" in sql
