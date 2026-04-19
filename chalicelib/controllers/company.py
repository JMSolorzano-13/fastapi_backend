import io
import json
from collections.abc import Iterable
from datetime import datetime
from typing import Any, Literal

from chalice import BadRequestError, ForbiddenError
from cryptography.hazmat.primitives import serialization
from pycfdi_credentials.certificate import Certificate, CertificateException
from pycfdi_credentials.private_key import PrivateKey, PrivateKeyException
from sqlalchemy import text
from sqlalchemy.orm import Session

from chalicelib.boto3_clients import s3_client, upload_fileobj_to_object_storage
from chalicelib.bus import get_global_bus
from chalicelib.controllers import ensure_list
from chalicelib.controllers.common import CommonController
from chalicelib.controllers.pdf_scraper import ScraperController
from chalicelib.controllers.permission import Ability, PermissionController, Role
from chalicelib.controllers.tenant.db import create_tenant_database_and_schema
from chalicelib.controllers.tenant.session import new_company_session_from_company_identifier
from chalicelib.controllers.tenant.utils import company_from_identifier
from chalicelib.controllers.user import UserController
from chalicelib.controllers.workspace import WorkspaceController
from chalicelib.exceptions import DocDefaultException
from chalicelib.logger import DEBUG, EXCEPTION, WARNING, log
from chalicelib.modules import Modules
from chalicelib.new.company.infra.company_repository_sa import CompanyRepositorySA
from chalicelib.new.config.infra import envars
from chalicelib.new.config.infra.envars.control import ISR_DEFAULT_PERCENTAGE
from chalicelib.new.shared.domain.event.event import CompanyWithSession
from chalicelib.new.shared.domain.event.event_type import EventType
from chalicelib.new.shared.domain.primitives import normalize_identifier
from chalicelib.new.utils.session import new_session
from chalicelib.schema.models import Company, Model, User
from chalicelib.schema.models.workspace import Workspace


def _get_route(workspace_id: int, company_id: int, ext: Literal["cer", "key", "txt"]):
    return f"ws_{workspace_id}/c_{company_id}.{ext}"


CertInfo = dict[str, Any]


class NotCertsError(DocDefaultException):
    "The company does not have certificates uploaded in the system"


class FIELError(DocDefaultException, BadRequestError):
    "FIEL Error"


class MismatchPassphrasePrivateKeyError(FIELError):
    """Invalid private key, maybe the passphrase is wrong"""


class InvalidPrivateKeyError(FIELError):
    "Not a proper private key"


class InvalidCertificateError(FIELError):
    "Not a proper certificate"


class NotAFIELCertificateError(FIELError):
    "The certificate is not a FIEL certificate"


class MismatchRFCError(FIELError):
    "The RFC in the certificate is not the same as the one in the company"


class ExpiredCertificateError(FIELError):
    "Certificate is expired"


class NotYetValidCertificateError(FIELError):
    "Certificate is not yet valid"


class MismatchPrivateKeyCertificateError(FIELError):
    "The private key does not match the certificate"


class DuplicatedRFCError(DocDefaultException, BadRequestError):
    """Company RFC already exists in another freemium workspace"""


class CompanyController(CommonController):
    model = Company

    @classmethod
    def get_cert_info(cls, company: Company, *, session: Session, context=None) -> CertInfo:
        """
        Get the certificate for a company.
        """
        user = context["user"]
        abilities = PermissionController.get_abilities(user, company, session=session)
        if Ability.UploadCerts not in abilities:
            raise ForbiddenError(
                f"{cls.log_records(user)} can not read certs in {cls.log_records(company)}"
            )
        session.add(company)
        cert, _key, _password = cls._get_certs(company)
        certificate = get_certificate_from_bytes(cert)
        return cls._parse_cert(certificate)

    @classmethod
    def _parse_cert(cls, certificate: Certificate) -> CertInfo:
        """Parse the certificate and return the data."""
        return {
            "rfc": certificate.subject.rfc,
            "name": certificate.subject.name,
            "not_before": certificate.valid_not_before,
            "not_after": certificate.valid_not_after,
            "serial_number": certificate.serial_number,
        }

    @classmethod
    @ensure_list
    def check_companies(cls, records: list[Model], *, session: Session, context=None):
        return True  # TODO: check workspace ownership

    @classmethod
    def _get_certs(cls, company: Company) -> tuple[bytes, bytes, bytes]:
        cer_file = io.BytesIO()
        key_file = io.BytesIO()
        txt_file = io.BytesIO()
        try:
            s3_client().download_fileobj(
                envars.S3_CERTS,
                _get_route(company.workspace_id, company.id, "cer"),
                cer_file,
            )
            s3_client().download_fileobj(
                envars.S3_CERTS,
                _get_route(company.workspace_id, company.id, "key"),
                key_file,
            )
            s3_client().download_fileobj(
                envars.S3_CERTS,
                _get_route(company.workspace_id, company.id, "txt"),  # TODO get from DB
                txt_file,
            )
        except Exception as e:
            raise NotCertsError() from e
        cer_file.seek(0)
        key_file.seek(0)
        txt_file.seek(0)
        return (
            cer_file.read(),
            key_file.read(),
            txt_file.read(),
        )

    @staticmethod
    def upload_certs(
        company: Company,
        cer: bytes,
        key: bytes,
        password: str,
        *,
        session: Session,
        context=None,
    ) -> dict[str, str]:
        session.add(company)
        # License
        if not company.active and company.workspace.is_active:
            raise ForbiddenError(
                f"{CompanyController.log_records(company.workspace)} is not active"
            )

        # Permissions
        user = context["user"]
        session.add(user)
        permission_controller = PermissionController()
        if company.workspace.owner_id != user.id:
            permission_controller.create_owner_permission(
                owner_id=company.workspace.owner_id, company_id=company.id, session=session
            )
        abilities = permission_controller.get_abilities(user, company, session=session)
        permission_controller = PermissionController()
        if company.workspace.owner_id != user.id:
            permission_controller.create_owner_permission(
                owner_id=company.workspace.owner_id, company_id=company.id, session=session
            )
        abilities = permission_controller.get_abilities(user, company, session=session)
        if Ability.UploadCerts not in abilities:
            raise ForbiddenError(
                f"{CommonController.log_records(user)} can not upload"
                f"certs in {CommonController.log_records(company)}"
            )
        certificate = get_certificate_and_validate_private_key(cer, key, password)
        assert_same_rfc_in_new_cer(company.rfc, certificate)
        upload_fileobj_to_object_storage(
            envars.S3_CERTS,
            _get_route(company.workspace_id, company.id, "cer"),
            io.BytesIO(cer),
        )
        upload_fileobj_to_object_storage(
            envars.S3_CERTS,
            _get_route(company.workspace_id, company.id, "key"),
            io.BytesIO(key),
        )

        txt_file = io.BytesIO(password.encode())
        upload_fileobj_to_object_storage(
            envars.S3_CERTS,
            _get_route(company.workspace_id, company.id, "txt"),
            txt_file,
        )
        is_new = True
        log(
            Modules.ACCOUNT,
            DEBUG,
            "UPLOAD_CERTS",
            {
                "company_id": company.id,
                "workspace_id": company.workspace_id,
                "user_id": user.id,
            },
        )
        if company.have_certificates or company.has_valid_certs:
            log(
                Modules.ACCOUNT,
                DEBUG,
                "COMPANY_ALREADY_HAS_CERTS",
                {
                    "company_id": company.id,
                },
            )
            is_new = False
        company.have_certificates = True
        company.has_valid_certs = True
        bus = get_global_bus()
        if not workspace_has_already_another_active_company(company) and is_new:
            bus.publish(
                EventType.REQUEST_RESTORE_TRIAL,
                user,
            )
        return {"Successful": "Fiel Uploaded"}

    @classmethod
    def get_company_ids(cls, records: list[Model], *, session: Session) -> set[int]:
        session.add_all(records)
        return {record.id for record in records}

    @classmethod
    def ensure_no_rfc_in_freemium_accounts(cls, workspace, rfc: str, session: Session) -> None:
        query = text(
            """
    SELECT
        workspace.identifier as wid,
        company.identifier as cid
    FROM
        company
        INNER JOIN workspace ON company.workspace_identifier = workspace.identifier
    WHERE
        company.rfc = :rfc
        AND (workspace.license::jsonb -> 'details' -> 'products' @> :products_json
        OR workspace.license::jsonb ->> 'stripe_status' <> :stripe_status)
"""
        )
        if rfc in envars.SPECIAL_RFCS:
            return
        params = {
            "rfc": rfc,
            "products_json": json.dumps([{"identifier": envars.VITE_REACT_APP_PRODUCT_TRIAL}]),
            "stripe_status": "active",  # TODO make it Enum
        }
        result = session.execute(query, params).fetchall()
        if len(result) >= envars.MAX_SAME_COMPANY_IN_TRIALS:
            workspace_company = [(row["wid"], row["cid"]) for row in result]
            log(
                Modules.ACCOUNT,
                WARNING,
                "DUPLICATED_FREEMIUM_RFC",
                {
                    "rfc": rfc,
                    "requester_workspace_id": workspace.identifier,
                    "already_in": workspace_company,
                },
            )

            raise DuplicatedRFCError()

    @staticmethod
    def publish_company_created_deferred(company_identifier: str) -> None:
        """
        Run COMPANY_CREATED bus handlers outside the HTTP request thread (Azure / LOCAL_INFRA=0).

        Opens a fresh global session and tenant session so handlers are not tied to the request
        Session lifecycle. Must run after the creating transaction has committed (e.g. FastAPI
        BackgroundTasks after dependency teardown).
        """
        bus = get_global_bus()
        try:
            cid = normalize_identifier(company_identifier)
            with new_session(comment="publish_company_created_deferred", read_only=True) as session:
                company = company_from_identifier(cid, session)
                with new_company_session_from_company_identifier(
                    company_identifier=company.identifier,
                    session=session,
                    read_only=False,
                ) as company_session:
                    bus.publish(
                        EventType.COMPANY_CREATED,
                        CompanyWithSession(company=company, company_session=company_session),
                    )
        except Exception as e:
            log(
                Modules.ACCOUNT,
                EXCEPTION,
                "COMPANY_CREATED_DEFERRED_FAILED",
                {
                    "company_identifier": company_identifier,
                    "exception": e,
                },
            )
            raise

    @staticmethod
    def create_from_certs(
        workspace_identifier,
        workspace_id,
        cer: bytes,
        key: bytes,
        password: str,
        *,
        session: Session,
        context=None,
        defer_company_created: bool = False,
    ) -> Company:
        """
        Create a company from the given certificate.
        Retrieves `name` and `rfc` from the certificate.
        """
        certificate = get_certificate_and_validate_private_key(cer, key, password)

        company = CompanyController.create(
            {
                "name": certificate.subject.name,
                "rfc": certificate.subject.rfc,
                "workspace_id": workspace_id,
                "workspace_identifier": workspace_identifier,
            },
            session=session,
            context=context,
        )
        session.flush()  # To have company.id
        CompanyController.upload_certs(
            company, cer, key, password, session=session, context=context
        )
        ScraperController.trigger_pdf_scraper(company.identifier, session)
        if defer_company_created:
            return company

        bus = get_global_bus()
        with new_company_session_from_company_identifier(
            company_identifier=company.identifier,
            session=session,
            read_only=False,
        ) as company_session:
            bus.publish(
                EventType.COMPANY_CREATED,
                CompanyWithSession(company=company, company_session=company_session),
            )

        return company

    @classmethod
    def is_freemium(cls, workspace: Workspace) -> bool:
        products = workspace.license["details"].get("products")
        if not products:
            return True
        return (
            any(
                product["identifier"] == envars.VITE_REACT_APP_PRODUCT_TRIAL
                for product in workspace.license["details"]["products"]
            )
            or workspace.license["stripe_status"] != "active"
        )

    @classmethod
    def create(
        cls,
        data: dict[str, Any],
        *,
        session: Session,
        context=None,
    ):
        user = context["user"]
        session.add(user)
        workspace = WorkspaceController.get(data["workspace_identifier"], session=session)
        data["workspace_identifier"] = workspace.identifier
        company_repo = CompanyRepositorySA(
            session,
        )
        rfc = data["rfc"]
        companies_in_workspace = company_repo.get_companies_in_workspace(
            workspace_identifier=workspace.identifier
        )
        for company_in_workspace in companies_in_workspace:
            if company_in_workspace.rfc == rfc and rfc != "PGD1009214W0":
                raise ForbiddenError(f"RFC {rfc} already in {cls.log_records(workspace)}")
        WorkspaceController.check_can_create_companies(workspace, session=session, context=context)
        if not WorkspaceController.user_is_owner_or_invited(user, workspace):
            raise ForbiddenError(
                f"{cls.log_records(user)} can not create company in {cls.log_records(workspace)}"
            )

        if CompanyController.is_freemium(workspace):
            CompanyController.ensure_no_rfc_in_freemium_accounts(workspace, rfc, session=session)

        company = super().create(data, session=session, context=context)
        cls.set_tenant_db_config(company)
        cls.create_database_and_schema(company)
        session.flush()  # Ensure company.id is available for roles creation
        cls.create_first_roles(user, company, session=session, context=context)
        return company

    @classmethod
    def set_tenant_db_config(cls, company: Company):
        """Set the tenant database configuration for the company"""
        company.tenant_db_schema = company.identifier
        company.tenant_db_name = envars.DB_NAME
        company.tenant_db_host = envars.DB_HOST
        company.tenant_db_port = envars.DB_PORT
        company.tenant_db_user = envars.DB_USER
        company.tenant_db_password = envars.DB_PASSWORD

    @classmethod
    def create_database_and_schema(cls, company: Company):
        """Create the database for the new company"""
        create_tenant_database_and_schema(company)

    @classmethod
    def create_first_roles(cls, user: User, company: Company, *, session: Session, context=None):
        """Generate the first roles for the owner user"""
        session.add(user)
        session.add(company)
        UserController.set_permissions(
            [user.email],
            {str(company.identifier): {Role.PAYROLL.name, Role.OPERATOR.name}},
            session=session,
            context=context,
        )

    @classmethod
    def _check_data(cls, record: Company, data: dict[str, Any], *, session: Session, context=None):
        super()._check_data(record, data, session=session, context=context)
        email_lists = (
            "emails_to_send_efos",
            "emails_to_send_errors",
            "emails_to_send_canceled",
        )
        for email_list in email_lists:
            if email_list in data and not isinstance(data[email_list], list):
                raise ValueError(f"{email_list} must be a list of emails")


def assert_valid_private_key_and_password(key: bytes, password: bytes) -> None:
    try:
        PrivateKey(key, password)
    except PrivateKeyException as exception:
        raise MismatchPassphrasePrivateKeyError() from exception
    except Exception as exception:
        raise InvalidPrivateKeyError() from exception


def assert_is_fiel(cert: Certificate) -> None:
    if cert.cert_type != Certificate.CertType.FIEL:
        raise NotAFIELCertificateError()


def get_certificate_from_bytes(cert_bytes: bytes) -> Certificate:
    try:
        return Certificate(cert_bytes)
    except (CertificateException, Exception) as exception:
        raise InvalidCertificateError() from exception


def assert_same_rfc_in_new_cer(rfc: str, cer: Certificate) -> None:
    if cer.subject.rfc != rfc:
        raise MismatchRFCError()


def workspace_has_already_another_active_company(current_company: Company) -> bool:
    workspace_brother_companies: Iterable[Company] = (
        company for company in current_company.workspace.companies if company != current_company
    )
    return any(
        company.active and company.have_certificates for company in workspace_brother_companies
    )


def get_certificate_and_validate_private_key(cer: bytes, key: bytes, password: str) -> Certificate:
    certificate = get_certificate_from_bytes(cer)
    assert_is_fiel(certificate)
    check_valid_certificate(certificate)

    password_b = password.encode("UTF-8")
    assert_valid_private_key_and_password(key, password_b)
    assert_key_matches_certificate(certificate, key, password)

    return certificate


def check_valid_certificate(certificate: Certificate) -> None:
    now = datetime.now()
    if certificate.valid_not_after < now:  # TODO MX Timezone
        raise ExpiredCertificateError()
    if certificate.valid_not_before > now:  # TODO MX Timezone
        raise NotYetValidCertificateError()


def assert_key_matches_certificate(certificate: Certificate, key: bytes, password: str) -> None:
    password_b = password.encode("UTF-8")
    private_key_instance = PrivateKey(key, password_b)

    key_public_obj = private_key_instance.private_key.to_cryptography_key().public_key()
    key_public_pem = key_public_obj.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    if key_public_pem != certificate.pub_key:
        raise MismatchPrivateKeyCertificateError()


def get_company_isr_percentage(company: Company):
    return company.data.get("isr_percentage", ISR_DEFAULT_PERCENTAGE)


def populate_company_emails(company: Company, email: str) -> None:
    """
    Populate email fields for a company with the given email.
    """
    email_list = [email]
    company.emails_to_send_efos = email_list
    company.emails_to_send_errors = email_list
    company.emails_to_send_canceled = email_list
