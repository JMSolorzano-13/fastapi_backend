import enum


class FileType(enum.Enum):
    CERTIFICATE = "certificate.cer"
    PRIVATE_KEY = "private.key"
    PASSPHRASE = "passphrase.txt"
