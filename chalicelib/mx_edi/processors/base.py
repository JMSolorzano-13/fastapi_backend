from collections.abc import Callable

from ..core import CFDI
from ..utils import ensure_list


class BaseProcessor:
    def _process(self, cfdi: CFDI) -> CFDI:  # pylint: disable=no-self-use
        return cfdi

    @ensure_list
    def process(self, cfdis: list[CFDI]):
        for cfdi in cfdis:
            self._process(cfdi)


class ProcessorRule:
    def __init__(self, name: str, expression: Callable):
        self.name = name
        self.expression = expression

    def evaluate(self, cfdi: CFDI) -> bool:
        return self.expression(cfdi)
