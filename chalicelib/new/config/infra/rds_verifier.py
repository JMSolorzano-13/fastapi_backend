from dataclasses import dataclass
from datetime import datetime, timedelta

from chalicelib.boto3_clients import cloudwatch_client
from chalicelib.logger import INFO, log
from chalicelib.modules import Modules
from chalicelib.new.config.infra import envars


@dataclass
class RDSVerifier:
    statistics_info_time_delta: timedelta
    statistics_info_period_seconds: int
    db_cluster_identifier: str
    was_not_ok: bool = False

    def is_ok_to_process(self, max_cpu_utilization: float) -> bool:
        if self.was_not_ok:
            # was not ok in previous check in this lambda call. Skipping for better performance."
            log(
                Modules.DB,
                INFO,
                "RDS_NOT_OK_TO_PROCESS",
            )
            return False
        if envars.LOCAL_INFRA:
            return True
        cpu_utilization = self.get_cpu_rds()
        is_ok = cpu_utilization < max_cpu_utilization
        if not is_ok:
            self.was_not_ok = True
            log(
                Modules.DB,
                INFO,
                "RDS_NOT_OK_TO_PROCESS",
                {
                    "cpu_utilization": cpu_utilization,
                    "max_cpu_utilization": max_cpu_utilization,
                },
            )
        return is_ok

    def get_cpu_rds(self):
        now = datetime.utcnow()
        response = cloudwatch_client().get_metric_statistics(
            Namespace="AWS/RDS",
            MetricName="CPUUtilization",
            Dimensions=[
                {"Name": "DBClusterIdentifier", "Value": self.db_cluster_identifier},
            ],
            StartTime=now - self.statistics_info_time_delta,
            EndTime=now,
            Period=self.statistics_info_period_seconds,
            Statistics=[
                "Average",
            ],
            Unit="Percent",
        )
        records = len(response["Datapoints"])
        return sum(float(x["Average"]) for x in response["Datapoints"]) / (records or 1)
