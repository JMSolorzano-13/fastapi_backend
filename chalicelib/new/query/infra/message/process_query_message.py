from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.shared.infra.message import SQSCompany, SQSMessage


class SQSProcessMessage(SQSMessage):
    query_identifier: Identifier


class SQSProcessPackageXMLRaw(SQSCompany):
    zip_path: str


class ProcessMetadataAndXmlFromZips(SQSCompany):
    metadata_zip_path: str
    xml_zip_path: str
