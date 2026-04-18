from dataclasses import dataclass

from pycfdi_credentials import Certificate, PrivateKey


@dataclass
class FIEL:
    certificate: Certificate
    private_key: PrivateKey
