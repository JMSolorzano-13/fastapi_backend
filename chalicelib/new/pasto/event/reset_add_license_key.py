from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.new.shared.infra.message import SQSMessage


class ADDResetLicenseKey(SQSMessage):
    license_key: Identifier
