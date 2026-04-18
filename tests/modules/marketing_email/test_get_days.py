import httpx
import pytest
from pydantic import EmailStr

from chalicelib.new.license.infra.siigo_marketing import Status, async_get_siigo_free_trial


@pytest.mark.third_party
@pytest.mark.parametrize(
    "email, expected_status",
    [
        # -1: Unconfirmed (Sin confirmar)
        ("jose_almaguer_almaguerctb2025@yopmail.com", Status.UNCONFIRMED),
        ("sailormoon900@sharklasers.com", Status.UNCONFIRMED),
        ("acxel.alejandro@correo.com", Status.UNCONFIRMED),
        # 0: Created (Creada):
        ("juanprueba22@yopmail.com", Status.CREATED),
        ("pruebagratis31@sharklasers.com", Status.CREATED),
        ("sailormoon910@sharklasers.com", Status.CREATED),
        ("pruebagratis30@sharklasers.com", Status.CREATED),
        # 1: Activated (Activada - Ya subió FIEL)
        ("abel_abarca_arandactb2025@yopmail.com", Status.ACTIVATED),
        ("abelardo_adame_alvaradoctb2025@yopmail.com", Status.ACTIVATED),
        ("jose_alvarez_aguirrectb2025@yopmail.com", Status.ACTIVATED),
        ("stivenpruebasctbpruebagratis2301225@sharklasers.com", Status.ACTIVATED),
        # 2: Purchased (Comprada)
        ("andrade90@sharklasers.com", Status.PURCHASED),
        ("amador90@sharklasers.com", Status.PURCHASED),
        ("sailormoon606@sharklasers.com", Status.PURCHASED),
        ("brendan.schuster37@yopmail.com", Status.PURCHASED),
        # 3: Expired (Expirada)
        ("nayeli.shanahan99@yopmail.com", Status.EXPIRED),
        ("contadorfreetrial26@yopmail.com", Status.EXPIRED),
        ("qactb1@gmail.com", Status.EXPIRED),
        ("Dariana267@yopmail.com", Status.EXPIRED),
    ],
)
@pytest.mark.asyncio
async def test_get_days(email: EmailStr, expected_status: Status):
    async with httpx.AsyncClient() as httpx_client:
        response = await async_get_siigo_free_trial(httpx_client, email)
        assert response.status == expected_status
