"""Test de memoria para _sync_efos con datos reales del SAT"""

import gc

import pytest
from memory_profiler import memory_usage
from sqlalchemy.orm import Session

from chalicelib.controllers.efos import EFOSController
from chalicelib.new.utils.session import with_session


@pytest.mark.skip(reason="Calcular memoria ram para _sync_efos")
def test_sync_efos_memoria_pico():
    """Mide memoria pico de _sync_efos con datos reales del SAT"""

    @with_session(read_only=False)
    def execute_sync(session: Session):
        updated_efos_list = EFOSController._get_updated_efos()
        session.query(EFOSController.model).delete()
        EFOSController.create_new_efos(updated_efos_list, session, context=None)
        return len(updated_efos_list)

    gc.collect()

    registros = [0]

    def wrapper():
        registros[0] = execute_sync()

    mem_usage = memory_usage(wrapper, interval=0.1, timeout=None)
    memoria_pico = max(mem_usage)

    assert memoria_pico < 512, "Excede de la memoria "

    # EXTRA INFO
    # print(f"Registros procesados: {registros[0]:,}")
    # print(f"Memoria pico:         {memoria_pico:.2f} MiB")
