from collections.abc import Iterable
from typing import Protocol

from chalicelib.new.package.domain.package import Package
from chalicelib.new.shared.domain.primitives import Identifier


class PackageRepository(Protocol):
    def save(self, package: Package): ...

    def get_from_sat_uuid(self, sat_uuid: Identifier) -> Package: ...

    def get_from_sat_uuids(self, sat_uuids: Iterable[str]) -> Iterable[Package]:
        return []
