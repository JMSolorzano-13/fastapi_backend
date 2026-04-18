from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from chalicelib.controllers import scale_to_super_user
from chalicelib.controllers.efos import EFOSController
from chalicelib.schema.models.efos import EFOS


def test_fis579_parse_csv_with_problematic_bytes():
    """Verifica que _parse_csv_content() decodifica correctamente bytes del SAT."""
    csv_path = Path(__file__).parent / "articulo-69-b" / "Listado_Completo_69-B.csv"

    with open(csv_path, "rb") as f:
        raw_content = f.read()

    data = EFOSController._parse_csv_content(raw_content)
    efos_list = EFOSController._get_efos_from_data(data)

    assert len(efos_list) > 0
    assert all(hasattr(efos, "rfc") for efos in efos_list)


@pytest.mark.slow
def test_sync_efos(session: Session):
    """Test completo start-to-end: descarga CSV del SAT y sincroniza."""
    assert session.query(EFOS).count() == 0, "This test asume no EFOS are loaded"
    context = scale_to_super_user()
    EFOSController.update_from_sat(context=context, session=session)
    assert session.query(EFOS).count() > 0, "At least one EFOS must be loaded from SAT"
