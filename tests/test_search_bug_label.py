import json

import pytest
from chalice.test import Client

token = "eyJraWQiOiI3XC9neERXMFJXNUNKMlwvNGZKbVNLZWhER2ZUTW1KajFURUU3WG0wUFBlVVU9IiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiI2NDU4MzQwOC04MDExLTcwZGEtMzczZC0yOTc1Zjk3NWJmYTIiLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiaXNzIjoiaHR0cHM6XC9cL2NvZ25pdG8taWRwLnVzLWVhc3QtMS5hbWF6b25hd3MuY29tXC91cy1lYXN0LTFfekF3a2VtdlBqIiwiY29nbml0bzp1c2VybmFtZSI6InRyZWszMTNuMkBtb3ptYWlsLmNvbSIsIm9yaWdpbl9qdGkiOiIzYTk3MmRkYi01YWUyLTQ0OTEtYmU3ZS00NDkxYTVhN2NmNWUiLCJhdWQiOiI0Z3VtcGRhMmpiaXRha2cwaHRiMWJkN2VmaiIsImV2ZW50X2lkIjoiZWYyZmE4MmUtYjE4NC00YzAwLWEzNmEtNzJjYTY1OGUzMGY4IiwidG9rZW5fdXNlIjoiaWQiLCJhdXRoX3RpbWUiOjE3NTY4MzQ4MDUsImV4cCI6MTc1NjgzODQwNSwiaWF0IjoxNzU2ODM0ODA1LCJqdGkiOiJiMzI0YTJkZS02MTEwLTRjMjYtODZmNy00M2JmMTc0MjNjZjkiLCJlbWFpbCI6InRyZWszMTNuMkBtb3ptYWlsLmNvbSJ9.jJt6pvdBME-AvIAAPwJop5lXhHAqWrr1-IcN2T2dU4t5_zE5s-eSQBwJlvSRNXF3qY-ADYHuXI9HC_x-8B5i61w0-x10Dr3ZXj1Eh-qVz8MNckyqLTSjvO89mMr8m3yvbZH71e56O3cid4sNUZ9qVXv4whvqwQl1nh5hfHPN1ukuBmvz5yRXwZKtRxDp115OMNKSJ2qYSPFFLFlalsj9kgo3IDosrlAcpreqlP-HfO3jzM9VgRSK306yvsRIqZWvfV9l4oOsEquW_uKMaoXVPdtYGjcsLl6KK6x46zpFo_v80cnd-H5hp-EjRNTd8_Xk_q8viURzojdPNlXq3Oe_NA"


@pytest.mark.skip(reason="test specifico Luis")
def test_search_bug_label(client: Client):
    response = client.http.post(
        "/CFDI/search",
        body=json.dumps(
            {
                "domain": [
                    ["company_identifier", "=", "013656c9-6f38-44c2-9ed4-f27007c22061"],
                    ["TipoDeComprobante", "=", "I"],
                    ["Estatus", "=", "true"],
                    ["MetodoPago", "=", "PUE"],
                    ["Version", "=", "4.0"],
                    ["FormaPago", "in", ["02", "03", "04", "05", "06", "28", "29"]],
                    ["UsoCFDIReceptor", "in", ["G03", "G01"]],
                    ["is_issued", "=", "false"],
                    ["ExcludeFromISR", "=", "false"],
                    ["FechaFiltro", ">=", "2024-03-01T00:00:00.000"],
                    ["FechaFiltro", "<", "2024-04-01T00:00:00.000"],
                ],
                "fields": [
                    "Fecha",
                    "UUID",
                    "Serie",
                    "Folio",
                    "RfcReceptor",
                    "NombreReceptor",
                    "RfcEmisor",
                    "NombreEmisor",
                    "TipoDeComprobante",
                    "FormaPago",
                    # "forma_pago_code",
                    "c_forma_pago.code",
                    "c_forma_pago.name",
                    "payments.FormaDePagoP",
                    "payments.c_forma_pago.name",
                    "MetodoPago",
                    "BaseIVA16",
                    "BaseIVA8",
                    "BaseIVA0",
                    "BaseIVAExento",
                    "RetencionesISRMXN",
                    # "base_isr",
                    "PaymentDate",
                    "ExcludeFromISR",
                    "ExcludeFromIVA",
                    "Version",
                    "is_too_big",
                    "Fecha",
                    "UsoCFDIReceptor",
                    "SubTotalMXN",
                    "DescuentoMXN",
                    "NetoMXN",
                ],
                "order_by": "Fecha desc",
                "limit": 30,
                "offset": 0,
            }
        ),
        headers={"Content-Type": "application/json", "access_token": token},
    )
    assert response.status_code == 200
    assert "data" in response.json_body
    assert isinstance(response.json_body["data"], list)
