import json

from chalicelib.new.shared.infra.message import SQSMessage


class MassiveExportEvent(SQSMessage):
    cfdi_export_identifier: dict[str, str | dict[str, str]]

    @classmethod
    def from_event(cls, event):
        return cls(cfdi_export_identifier=json.loads(event.body)["cfdi_export_identifier"])
