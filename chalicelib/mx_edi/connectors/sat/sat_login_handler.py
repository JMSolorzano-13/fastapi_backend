import logging
from datetime import datetime, timedelta
from uuid import uuid1

from . import templates, utils
from .certificate_handler import CertificateHandler
from .response_parsers import LoginParser

_logger = logging.getLogger(__name__)


class SATLoginHandler:
    _token: str = ""
    _token_expires: datetime
    _certificate_handler: CertificateHandler

    def __init__(self, certificate_handler):
        self._certificate_handler = certificate_handler
        self._token_expires = datetime.min

    @property
    def token(self) -> str:
        self.login()
        return self._token

    def _token_expired(self) -> bool:
        """Checks if the token expiration date is yet to come

        Returns:
            bool: True if not token or if is expired
        """
        if not self._token or not self._token_expires or datetime.utcnow() > self._token_expires:
            _logger.debug("Token expired")
            return True
        return False

    def login(
        self,
        created: datetime = None,
        expires: datetime = None,
        uuid: str = None,
        force: bool = False,
    ):
        """If the current token is invalid, tries to login

        Args:
            created (datetime, optional): Creation date to be used in the session.
                                          Defaults to datetime.utcnow().
            expires (datetime, optional): Expiration date to be used in the session.
                                          Defaults to datetime.utcnow()+timedelta(minutes=5).
            uuid (str, optional): UUID date to be used in the session.
                                  Defaults to f'uuid-{uuid1()}-1'.
        """
        if not self._token_expired() and not force:
            return
        _logger.debug("Token expired, creating a new one")
        created = created or datetime.utcnow()
        expires = expires or created + timedelta(minutes=5)
        uuid = uuid or f"uuid-{uuid1()}-1"
        self._login(created, expires, uuid)
        self._token_expires = expires
        _logger.debug("New token created")

    def _login(self, created: datetime, expires: datetime, uuid: str, request_content: str = None):
        """Send login request to the SAT

        Args:
            created (datetime): Creation date to be used in the session
            expires (datetime): Expiration date to be used in the session
            uuid (str): UUID date to be used in the session

        Raises:
            RequestException: If there was an error in the request
        """
        request_content = request_content or self._get_login_soap_body(created, expires, uuid)
        response = utils.consume(
            "http://DescargaMasivaTerceros.gob.mx/IAutenticacion/Autentica",
            "https://cfdidescargamasivasolicitud.clouda.sat.gob.mx/Autenticacion/Autenticacion.svc",
            request_content,
        )
        utils.check_response(response)
        response_clean = utils.remove_namespaces(response.content.decode("UTF-8"))
        data = LoginParser.parse(response_clean)

        self._token = data["token"]

    def _get_login_soap_body(
        self, created_object: datetime, expires_object: datetime, uuid: str
    ) -> str:
        """Creates the request body to be used in login

        Args:
            created_object (datetime): Creation date to be used in the session
            expires_object (datetime): Expiration date to be used in the session
            uuid (str): UUID date to be used in the session

        Returns:
            str: Content body
        """
        created = created_object.isoformat()
        expires = expires_object.isoformat()
        timestamp = utils.prepare_template(
            templates.Timestamp,
            {
                "created": created,
                "expires": expires,
            },
        )
        digest_value = utils.digest(timestamp)
        signed_info = utils.prepare_template(
            templates.SignedInfo,
            {
                "uri": "#_0",
                "digest_value": digest_value,
            },
        )
        signature_value = self._certificate_handler.sign(signed_info)
        _logger.debug(
            """Creating Login Envelope with the next data
            "created": %s,
            "expires": %s,
            "uuid": %s,
        """,
            created,
            expires,
            uuid,
        )
        return utils.prepare_template(
            templates.LoginEnvelope,
            {
                "binary_security_token": self._certificate_handler.cert,
                "digest_value": digest_value,
                "signature_value": signature_value,
                "uuid": uuid,
                "timestamp_node": timestamp,
                "signed_info_node": signed_info,
            },
        )
