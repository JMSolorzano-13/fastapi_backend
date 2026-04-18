from abc import ABC

import boto3

from chalicelib.mx_edi.connectors.sat.sat_connector import SATConnector
from chalicelib.new.config.infra.envars import envars
from chalicelib.new.ws_sat.fiel_repository_s3 import get_fiel_from_wid_cid


class WSRepo(ABC):  # noqa E501
    def get_sat_connector(self, wid: int, cid: int) -> SATConnector:
        fiel = get_fiel_from_wid_cid(
            s3_client=boto3.client("s3"), bucket_url=envars.S3_CERTS, wid=wid, cid=cid
        )
        return SATConnector(
            fiel["certificate"].to_der(),
            fiel["private_key"].content,
            fiel["private_key"].passphrase,
        )
