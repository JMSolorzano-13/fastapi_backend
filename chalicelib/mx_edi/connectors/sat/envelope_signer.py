import logging

from . import templates, utils
from .certificate_handler import CertificateHandler

_logger = logging.getLogger(__name__)


class EnvelopeSigner:
    certificate_handler: CertificateHandler

    def __init__(self, certificate_handler: CertificateHandler):
        self.certificate_handler = certificate_handler

    def create_common_envelope(self, template: str, data: dict) -> str:
        _logger.debug("Creating Envelope")
        _logger.debug("%s", template)
        _logger.debug("%s", data)
        query_data_signature = utils.prepare_template(template, data)
        data["signature"] = ""
        query_data = utils.prepare_template(template, data)
        digest_value = utils.digest(query_data)
        signed_info = utils.prepare_template(
            templates.SignedInfo,
            {
                "uri": "",
                "digest_value": digest_value,
            },
        )
        key_info = utils.prepare_template(
            templates.KeyInfo,
            {
                "issuer_name": self.certificate_handler.certificate.issuer,
                "serial_number": self.certificate_handler.certificate.get_serial_number(),
                "certificate": self.certificate_handler.cert.replace("\n", ""),
            },
        )
        signature_value = self.certificate_handler.sign(signed_info)
        signature = utils.prepare_template(
            templates.Signature,
            {
                "signed_info": signed_info,
                "signature_value": signature_value,
                "key_info": key_info,
            },
        )
        envelope_content = utils.prepare_template(
            query_data_signature,
            {
                "signature": signature,
            },
        )
        envelope = utils.prepare_template(
            templates.Envelope,
            {
                "content": envelope_content,
            },
        )
        _logger.debug("Final Envelope")
        _logger.debug("%s", envelope)
        return envelope
