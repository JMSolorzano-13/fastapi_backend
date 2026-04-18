from contextlib import contextmanager
from tempfile import NamedTemporaryFile, TemporaryDirectory
from zipfile import ZipFile

from chalicelib.new.package.domain.package import Package


class ZipProcessor:
    @contextmanager
    def decompress_temporary_path(self, package: Package) -> str:
        with TemporaryDirectory() as temp_dir:
            with NamedTemporaryFile(suffix=".zip") as temp_file:
                temp_file.write(package.zip_content)
                temp_file.seek(0)
                with ZipFile(temp_file.name, "r") as zip_file:
                    zip_file.extractall(temp_dir)
            yield temp_dir
