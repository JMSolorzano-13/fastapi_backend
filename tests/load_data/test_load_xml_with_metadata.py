import os

from sqlalchemy.orm import Session

from chalicelib.new.query.domain.metadata import Metadata
from chalicelib.new.query.domain.metadata_processor import MetadataProcessor
from chalicelib.new.query.domain.xml_processor import XMLProcessor
from chalicelib.new.query.infra.cfdi_repository_sa import CFDIRepositorySA
from chalicelib.schema.models.company import Company
from chalicelib.schema.models.tenant.cfdi import CFDI
from tests.load_data.metadata_generator import create_metadata

XMLS_PATH = "tests/load_data/xml_test"
METADATA = "tests/metadata"


def read_files_from_directory(directory_path: str) -> list[str]:
    xmls = []
    for filename in os.listdir(directory_path):
        if filename.endswith(".xml"):
            full_path = os.path.join(directory_path, filename)
            with open(full_path, encoding="utf-8") as f:
                xmls.append(f.read())
    return xmls


def test_load_xmls_with_metadata_db_vigente_to_cancel(
    company_other: Company,
    company_session_other: Session,
    benchmark,
):
    assert company_session_other.query(CFDI).count() == 0

    xml_content = read_files_from_directory(XMLS_PATH)
    cfdi_repo = CFDIRepositorySA(company_session_other)

    # Primera metadata con estatus=1
    metadata_dir = create_metadata(
        xml_content,
        company_other.identifier,
        estatus="1",
    )
    metadata = Metadata.from_txt(metadata_dir)

    pm = MetadataProcessor(cfdi_repo=cfdi_repo, metadata_repo=None, query_repo=None, bus=None)

    pm._process_metadata(
        metadatas=metadata,
        company_identifier=company_other.identifier,
        rfc=company_other.rfc,
    )

    def process_xmls():
        processor = XMLProcessor(
            cfdi_repo=cfdi_repo, xml_repo=None, company_session=company_session_other
        )
        processor.process_xml_files(
            company_identifier=company_other.identifier,
            xmls_contents=xml_content,
            rfc=company_other.rfc,
        )
        company_session_other.commit()

    # Benchmark PRIMERA carga
    benchmark(process_xmls)

    count_after_first = company_session_other.query(CFDI).count()
    assert count_after_first == len(xml_content)

    # Recuperar todos los CFDIs creados
    cfdis_first = company_session_other.query(CFDI).all()
    assert all(c.Estatus is True for c in cfdis_first)

    # Guardar updated_at antes de la segunda carga
    timestamps_before_second = {(c.is_issued, str(c.UUID)): c.updated_at for c in cfdis_first}

    # Segunda metadata con estatus=0
    metadata_dir_2 = create_metadata(xml_content, company_other.identifier, estatus="0")
    metadata2 = Metadata.from_txt(metadata_dir_2)

    # Limpiar tabla temporal si aplica
    cursor = company_session_other.connection().connection.cursor()
    cursor.execute('DROP TABLE IF EXISTS "tmp_cfdi"')

    pm._process_metadata(
        metadatas=metadata2, company_identifier=company_other.identifier, rfc=company_other.rfc
    )

    # Segunda carga SIN benchmark
    def process_xmls_second():
        processor = XMLProcessor(
            cfdi_repo=cfdi_repo, xml_repo=None, company_session=company_session_other
        )
        processor.process_xml_files(
            company_identifier=company_other.identifier,
            xmls_contents=xml_content,
            rfc=company_other.rfc,
        )
        company_session_other.commit()

    process_xmls_second()

    count_after_second = company_session_other.query(CFDI).count()
    assert count_after_second == count_after_first, (
        "La segunda carga no debería insertar duplicados"
    )

    # Recuperar CFDIs tras segunda carga
    cfdis_second = company_session_other.query(CFDI).all()

    for c in cfdis_second:
        assert c.Estatus is False, "Estatus no cambió a False"
        assert c.updated_at > timestamps_before_second[(c.is_issued, str(c.UUID))], (
            "updated_at  se actualizó"
        )


def test_load_xmls_with_metadata_db_cancel_to_vigente(
    company_other: Company, company_session_other: Session, benchmark
):
    assert company_session_other.query(CFDI).count() == 0

    xml_content = read_files_from_directory(XMLS_PATH)
    cfdi_repo = CFDIRepositorySA(company_session_other)

    # Primera metadata con estatus=1
    metadata_dir = create_metadata(
        xml_content,
        company_other.identifier,
        estatus="0",
    )
    metadata = Metadata.from_txt(metadata_dir)

    pm = MetadataProcessor(cfdi_repo=cfdi_repo, metadata_repo=None, query_repo=None, bus=None)

    pm._process_metadata(
        metadatas=metadata, company_identifier=company_other.identifier, rfc=company_other.rfc
    )

    def process_xmls():
        processor = XMLProcessor(
            cfdi_repo=cfdi_repo, xml_repo=None, company_session=company_session_other
        )
        processor.process_xml_files(
            company_identifier=company_other.identifier,
            xmls_contents=xml_content,
            rfc=company_other.rfc,
        )
        company_session_other.commit()

    # Benchmark PRIMERA carga
    benchmark(process_xmls)

    company_session_other.commit()

    count_after_first = company_session_other.query(CFDI).count()
    assert count_after_first == len(xml_content)

    # Recuperar todos los CFDIs creados
    cfdis_first = company_session_other.query(CFDI).all()
    assert all(c.Estatus is False for c in cfdis_first)

    # Guardar updated_at antes de la segunda carga
    timestamps_before_second = {(c.is_issued, str(c.UUID)): c.updated_at for c in cfdis_first}

    original_values = {}
    for c in cfdis_first:
        original_values[(c.is_issued, str(c.UUID))] = {
            "xml_content": c.xml_content,
            "Version": c.Version,
            "Total": c.Total,
            "SubTotal": c.SubTotal,
            "TipoDeComprobante": c.TipoDeComprobante,
        }

    # Segunda metadata con estatus=0
    metadata_dir_2 = create_metadata(xml_content, company_other.identifier, estatus="1")
    metadata2 = Metadata.from_txt(metadata_dir_2)

    # Limpiar tabla temporal si aplica
    cursor = company_session_other.connection().connection.cursor()
    cursor.execute('DROP TABLE IF EXISTS "tmp_cfdi"')

    pm._process_metadata(
        metadatas=metadata2, company_identifier=company_other.identifier, rfc=company_other.rfc
    )

    # Segunda carga SIN benchmark
    def process_xmls_second():
        processor = XMLProcessor(
            cfdi_repo=cfdi_repo, xml_repo=None, company_session=company_session_other
        )
        processor.process_xml_files(
            company_identifier=company_other.identifier,
            xmls_contents=xml_content,
            rfc=company_other.rfc,
        )
        company_session_other.commit()

    process_xmls_second()

    count_after_second = company_session_other.query(CFDI).count()
    assert count_after_second == count_after_first, (
        "La segunda carga no debería insertar duplicados"
    )

    # Recuperar CFDIs tras segunda carga
    cfdis_second = company_session_other.query(CFDI).all()

    for c in cfdis_second:
        assert c.Estatus is False
        assert c.updated_at == timestamps_before_second[(c.is_issued, str(c.UUID))]

        original_data = original_values[(c.is_issued, str(c.UUID))]
        assert c.xml_content == original_data["xml_content"]
        assert c.Version == original_data["Version"]
        assert c.Total == original_data["Total"]
        assert c.SubTotal == original_data["SubTotal"]
        assert c.TipoDeComprobante == original_data["TipoDeComprobante"]
