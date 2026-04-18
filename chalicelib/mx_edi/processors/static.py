from ..core import CFDI
from .base import BaseProcessor, ProcessorRule


class StaticProcessor(BaseProcessor):
    rules = [
        ProcessorRule(
            "TipoDeComprobante_I_MetodoPago_PPD",
            lambda cfdi: cfdi.TipoDeComprobante == "I"
            and cfdi.MetodoPago == "PPD"
            and cfdi.FormaPago != "99",
        ),
        ProcessorRule(
            "TipoDeComprobante_I_MetodoPago_PUE",
            lambda cfdi: cfdi.TipoDeComprobante == "I"
            and cfdi.MetodoPago == "PUE"
            and cfdi.FormaPago == "99",
        ),
        ProcessorRule(
            "TipoDeComprobante_E_MetodoPago_PPD",
            lambda cfdi: cfdi.TipoDeComprobante == "E"
            and (cfdi.MetodoPago == "PPD" or cfdi.FormaPago == "99"),
        ),
        ProcessorRule(
            "TipoDeComprobante_E_CfdiRelacionados_None",
            lambda cfdi: cfdi.TipoDeComprobante == "E" and not cfdi.CfdiRelacionados,
        ),
    ]

    def _process(self, cfdi: CFDI):
        cfdi.add_extra("static_rules", {rule.name: rule.evaluate(cfdi) for rule in self.rules})
