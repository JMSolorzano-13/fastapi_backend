import enum


class VerifyStatusCode(enum.Enum):
    INVALID_USER = 300  # "Usuario No Válido"
    MALFORMED_XML = 301  # "XML Mal Formado"
    MALFORMED_STAMP = 302  # "Sello Mal Formado"
    STAMP_DOES_NOT_CORRESPOND_TO_RFC_APPLICANT = 303  # "Sello no corresponde con RfcSolicitante"
    REVOKED_OR_EXPIRED_CERTIFICATE = 304  # "Certificado Revocado o Caduco"
    INVALID_CERTIFICATE = 305  # "Certificado Inválido"
    SAT_ERROR = 404  # "Error del SAT"
    REQUEST_RECEIVED_SUCCESSFULLY = 5000  # "Solicitud recibida con éxito"
    INFORMATION_NOT_FOUND = 5004  # "No se encontró la información"


class VerifyQueryStatusCode(enum.Enum):
    SAT_ERROR = 0  # Error del SAT
    REQUEST_RECEIVED_SUCCESSFULLY = 5000  # Solicitud recibida con éxito
    LIFETIME_REQUESTS_EXHAUSTED = 5002  # Se agotó las solicitudes de por vida
    MAXIMUM_LIMIT = 5003  # Tope máximo
    INFORMATION_NOT_FOUND = 5004  # No se encontró la información
    DUPLICATE_REQUEST = 5005  # Solicitud duplicada
    UNCONTROLLED_ERROR = 404  # Error no Controlado


class VerifyQueryStatus(enum.Enum):
    UNKNOWN = 0
    ACCEPTED = 1
    IN_PROCESS = 2
    FINISHED = 3
    ERROR = 4
    REJECTED = 5
    EXPIRED = 6
