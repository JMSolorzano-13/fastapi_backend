from sqlalchemy.orm import Session

from chalicelib.controllers.common import CommonController
from chalicelib.new.cfdi_processor.infra.cfdi_export_repository_sa import CFDIExportRepositorySA
from chalicelib.schema.models import CfdiExport


class CfdiExportController(CommonController):
    model = CfdiExport

    @classmethod
    def get_cfdi_export_repository(cls, *, session: Session) -> CFDIExportRepositorySA:
        return CFDIExportRepositorySA(session=session)
