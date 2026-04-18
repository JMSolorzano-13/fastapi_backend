import enum


class SendStatusCode(enum.IntEnum):
    INVALID_USER = 300  # Usuario No Válido
    MALFORMED_XML = 301  # XML Mal Formado
    MALFORMED_STAMP = 302  # Sello Mal Formado
    STAMP_DOES_NOT_CORRESPOND_WITH_RFC_APPLICANT = 303  # Sello no corresponde con Rfc Solicitante
    REVOKED_OR_EXPIRED_CERTIFICATE = 304  # Certificado Revocado o Caduco
    INVALID_CERTIFICATE = 305  # Certificado Inválido
    DOWNLOAD_REQUEST_RECEIVED_SUCCESSFULLY = 5000  # Solicitud de descarga recibida con éxito
    UNAUTHORIZED_THIRD_PARTY = 5001  # Tercero no autorizado
    LIFETIME_APPLICATIONS_HAVE_BEEN_EXHAUSTED = 5002  # Se han agotado las solicitudes de por vida
    ALREADY_REGISTERED_QUERY = 5005  # Ya se tiene una solicitud registrada
    INTERNAL_ERROR = 5006  # Error interno
    UNKNOWN = 404  # Desconocido
