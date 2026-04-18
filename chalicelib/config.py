import pdfkit  # type: ignore

from chalicelib.new.config.infra import envars

PAGE_SIZE = 80


def get_pdfkit_configuration():
    return pdfkit.configuration(
        wkhtmltopdf=envars.WKHTMLTOPDF_PATH,
    )
