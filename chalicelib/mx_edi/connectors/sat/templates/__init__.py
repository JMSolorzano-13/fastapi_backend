from importlib import resources

from . import common, download, login, query, verify

Envelope = resources.read_text(common, "Envelope.xml")
KeyInfo = resources.read_text(common, "KeyInfo.xml")
Signature = resources.read_text(common, "Signature.xml")
SignedInfo = resources.read_text(common, "SignedInfo.xml")
Timestamp = resources.read_text(login, "Timestamp.xml")
LoginEnvelope = resources.read_text(login, "Envelope.xml")
SolicitaDescarga = resources.read_text(query, "SolicitaDescarga.xml")
SolicitaDescargaEmitidos = resources.read_text(query, "SolicitaDescargaEmitidos.xml")
SolicitaDescargaRecibidos = resources.read_text(query, "SolicitaDescargaRecibidos.xml")
SolicitaDescargaFolio = resources.read_text(query, "SolicitaDescargaFolio.xml")
VerificaSolicitudDescarga = resources.read_text(verify, "VerificaSolicitudDescarga.xml")
PeticionDescargaMasivaTercerosEntrada = resources.read_text(
    download, "PeticionDescargaMasivaTercerosEntrada.xml"
)
