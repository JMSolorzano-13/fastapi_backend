from dataclasses import dataclass
from logging import DEBUG, INFO

from sqlalchemy import text
from sqlalchemy.orm import Session

from chalicelib.logger import log
from chalicelib.modules import Modules
from chalicelib.new.query.infra.temp_table_sa import temp_table_as
from chalicelib.new.shared.domain.primitives import Identifier
from chalicelib.schema.models.tenant import CFDI as CFDIORM
from chalicelib.schema.models.tenant import DoctoRelacionado as DoctoRelacionadoORM


@dataclass
class BalanceUpdater:
    company_session: Session

    def update_balances(self, company_identifier: Identifier):
        log(
            Modules.PROCESS_PAYMENTS,
            DEBUG,
            "PROCESSING",
            {"company_identifier": company_identifier},
        )

        cfdi_table = CFDIORM.get_specific_table(company_identifier)
        payment_relation_table = DoctoRelacionadoORM.get_specific_table(company_identifier)

        self.company_session.execute(
            text(
                f"""
            LOCK TABLE "{cfdi_table}" IN EXCLUSIVE MODE;
            LOCK TABLE "{payment_relation_table}" IN EXCLUSIVE MODE;
            """
            )
        )
        self._apply_pending_payments(cfdi_table, payment_relation_table)
        self._revert_payments_cancelled(cfdi_table, payment_relation_table)
        # Is NOT necessary to mark as available payments no longer applied
        # when the CFDI related is cancelled. This is because SAT requires
        # to first cancel the payments associated to the CFDI and then
        # cancel the CFDI itself. So, the payments will be cancelled before
        # the CFDI
        self.company_session.commit()

        log(
            Modules.PROCESS_PAYMENTS,
            DEBUG,
            "PROCESSED",
            {"company_identifier": company_identifier},
        )

    def _apply_pending_payments(self, cfdi_table: str, payment_relation_table: str):
        pending_payments_query = f"""
        SELECT
            identifier,
            "UUID_related",
            "ImpPagado"
        FROM
            "{payment_relation_table}" pr
            JOIN "{cfdi_table}" i ON i."UUID" = pr."UUID_related"
                AND i."Estatus"
            JOIN "{cfdi_table}" p ON p."UUID" = pr."UUID"
                AND p."Estatus"
        WHERE
            NOT pr.applied;
        """
        pending_table = temp_table_as(self.company_session, "pending", pending_payments_query)

        if not self.company_session.execute(
            text(f'SELECT EXISTS (SELECT 1 FROM "{pending_table}" LIMIT 1)')
        ).scalar():
            log(
                Modules.PROCESS_PAYMENTS,
                INFO,
                "NOT_PENDING_PAYMENTS",
                {"company_table": cfdi_table},
            )
            return
        log(
            Modules.PROCESS_PAYMENTS,
            INFO,
            "APPLY_PENDING_PAYMENTS",
            {"company_table": cfdi_table},
        )

        sql = text(f"""/* Apply pending payments */
        UPDATE "{cfdi_table}" i
        SET balance = balance - pg.total_amount
        FROM (
            SELECT
                "UUID_related",
                SUM("ImpPagado") AS total_amount
            FROM "{pending_table}"
            GROUP BY "UUID_related"
        ) pg
        WHERE
            "UUID" = pg."UUID_related"
        """)
        self.company_session.execute(sql)
        sql = text(f"""/* Mark payments as applied */
        UPDATE
            "{payment_relation_table}" pr
        SET
            applied = TRUE
        FROM
            "{pending_table}" pending
        WHERE
            pr.identifier = pending.identifier
        """)
        self.company_session.execute(sql)

    def _revert_payments_cancelled(self, cfdi_table: str, payment_relation_table: str):
        sql = text(f""" /* Revert payments cancelled */
        WITH pending AS (
            UPDATE "{payment_relation_table}" pr
            SET applied = false
            FROM "{cfdi_table}" i
            WHERE
                applied
                AND i."UUID" = pr."UUID"
                AND NOT i."Estatus"
            RETURNING
                pr."UUID_related",
                pr."ImpPagado"
        )
        UPDATE "{cfdi_table}" i
        SET balance = balance + pg.total_amount
        FROM (
            SELECT
                "UUID_related",
                SUM("ImpPagado") AS total_amount
            FROM pending
            GROUP BY "UUID_related"
        ) pg
        WHERE
            "UUID" = pg."UUID_related"
        """)
        self.company_session.execute(sql)
