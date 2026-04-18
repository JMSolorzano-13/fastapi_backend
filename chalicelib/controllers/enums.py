import enum
from enum import auto

from chalicelib.modules import NameEnum


class ResumeType(NameEnum):
    BASIC = auto()
    N = auto()
    P = auto()
    S = auto()
    PAYMENT_WITH_DOCTOS = auto()


class FormaPago(enum.StrEnum):
    EFECTIVO = "01"
    CHEQUE_NOMINATIVO = "02"
    TRANSFERENCIA_ELECTRONICA_DE_FONDOS = "03"
    TARJETA_DE_CREDITO = "04"
    MONEDERO_ELECTRONICO = "05"
    DINERO_ELECTRONICO = "06"
    VALES_DE_DESPENSA = "08"
    DACION_EN_PAGO = "12"
    PAGO_POR_SUBROGACION = "13"
    PAGO_POR_CONSIGNACION = "14"
    CONDONACION = "15"
    COMPENSACION = "17"
    NOVACION = "23"
    CONFUSION = "24"
    REMISION_DE_DEUDA = "25"
    PRESCRIPCION_O_CADUCIDAD = "26"
    A_SATISFACCION_DEL_ACREEDOR = "27"
    TARJETA_DE_DEBITO = "28"
    TARJETA_DE_SERVICIOS = "29"
    APLICACION_DE_ANTICIPOS = "30"
    INTERMEDIARIO_PAGOS = "31"
    POR_DEFINIR = "99"

    @classmethod
    def bancarizadas(cls):
        return {
            cls.CHEQUE_NOMINATIVO,
            cls.TRANSFERENCIA_ELECTRONICA_DE_FONDOS,
            cls.TARJETA_DE_CREDITO,
            cls.MONEDERO_ELECTRONICO,
            cls.DINERO_ELECTRONICO,
            cls.TARJETA_DE_DEBITO,
            cls.TARJETA_DE_SERVICIOS,
        }

    @property
    def bancarizada(self):
        return self in self.bancarizadas()

    @classmethod
    def no_bancarizadas(cls):
        return set(cls.__members__.values()) - cls.bancarizadas()


class UsoCFDI(enum.StrEnum):
    ADQUISICION_DE_MERCANCIAS = "G01"
    DEVOLUCIONES_DESCUENTOS_O_BONIFICACIONES = "G02"
    GASTOS_EN_GENERAL = "G03"
    CONSTRUCCIONES = "I01"
    MOBILIARIO_Y_EQUIPO_DE_OFICINA_POR_INVERSIONES = "I02"
    EQUIPO_DE_TRANSPORTE = "I03"
    EQUIPO_DE_COMPUTO_Y_ACCESORIOS = "I04"
    DADOS_TROQUELES_MOLDES_MATRICES_Y_HERRAMENTAL = "I05"
    COMUNICACIONES_TELEFONICAS = "I06"
    COMUNICACIONES_SATELITALES = "I07"
    OTRA_MAQUINARIA_Y_EQUIPO = "I08"
    HONORARIOS_MEDICOS_DENTALES_Y_GASTOS_HOSPITALARIOS = "D01"
    GASTOS_MEDICOS_POR_INCAPACIDAD_O_DISCAPACIDAD = "D02"
    GASTOS_FUNERALES = "D03"
    DONATIVOS = "D04"
    INTERESES_REALES_EFECTIVAMENTE_PAGADOS_POR_CREDITOS_HIPOTECARIOS_CASA_HABITACION = "D05"
    APORTACIONES_VOLUNTARIAS_AL_SAR = "D06"
    PRIMAS_POR_SEGUROS_DE_GASTOS_MEDICOS = "D07"
    GASTOS_DE_TRANSPORTACION_ESCOLAR_OBLIGATORIA = "D08"
    DEPOSITOS_EN_CUENTAS_PARA_EL_AHORRO_PRIMAS_QUE_TENGAN_COMO_BASE_PLANES_DE_PENSIONES = "D09"
    PAGOS_POR_SERVICIOS_EDUCATIVOS_COLEGIATURAS = "D10"
    SIN_EFECTOS_FISCALES = "S01"
    PAGOS = "CP01"
    NOMINA = "CN01"

    @classmethod
    def bancarizadas(cls):
        return {
            cls.ADQUISICION_DE_MERCANCIAS,
            cls.GASTOS_EN_GENERAL,
        }

    @classmethod
    def no_bancarizadas(cls):
        return set(cls.__members__.values()) - cls.bancarizadas()
