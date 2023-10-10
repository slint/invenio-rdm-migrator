# -*- coding: utf-8 -*-
#
# Copyright (C) 2023 CERN.
#
# Invenio-RDM-Migrator is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""PostgreSQL Execute load."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from ....logging import FailedTxLogger
from ...base import Load
from .operations import OperationType


@dataclass
class LoadStats:
    """Loading statistics."""

    start: Optional[datetime] = None
    tx: int = 0
    ops: int = 0

    @property
    def duration(self) -> Optional[timedelta]:
        """Total stats gathering duration."""
        if self.start:
            return datetime.utcnow() - self.start

    @property
    def tx_rate(self) -> float:
        """Get transaction rate per minute."""
        duration = self.duration
        if duration:
            return self.tx / (duration.total_seconds() / 60)
        return 0

    @property
    def ops_rate(self) -> float:
        """Get operations rate per minute."""
        duration = self.duration
        if duration:
            return self.ops / (duration.total_seconds() / 60)
        return 0

    def __str__(self):
        """Return loading stats rates, totals, and duration."""
        return (
            f"<LoadStats("
            f"{self.tx_rate:.2f} tx/min ({self.tx}), "
            f"{self.ops_rate:.2f} ops/min ({self.ops}), "
            f"[{self.duration}])>"
        )


class PostgreSQLTx(Load):
    """PostgreSQL COPY load."""

    def __init__(
        self, db_uri, _session=None, dry=True, raise_on_db_error=False, **kwargs
    ):
        """Constructor."""
        self.db_uri = db_uri
        self.dry = dry
        self.raise_on_db_error = raise_on_db_error
        self._session = _session
        self._tx_logger = None
        self._stats = LoadStats()
        super().__init__(**kwargs)

    @property
    def tx_logger(self):
        """Failed transactions logger."""
        if self._tx_logger is None:
            self._tx_logger = FailedTxLogger.get_logger()
        return self._tx_logger

    @property
    def session(self):
        """DB session."""
        if self._session is None:
            session_kwargs = dict(bind=create_engine(self.db_uri))
            if self.dry:
                session_kwargs["join_transaction_mode"] = "create_savepoint"
            self._session = Session(**session_kwargs)
        return self._session

    def _cleanup(self, db=False):
        """No cleanup."""
        self.logger.debug("PostgreSQLExecute does not implement _cleanup()")
        pass

    def _load(self, transactions):
        """Performs the operations of a group transaction."""
        exec_kwargs = dict(execution_options={"synchronize_session": False})

        outer_trans = None
        if self.dry:
            self.logger.warn("Running Tx loading in dry mode inside transaction!")
            outer_trans = self.session.begin()
        try:
            self._stats.start = datetime.utcnow()
            for action in transactions:
                tx = action.data.tx
                self.logger.info(
                    f"BEGIN | [{action.name}] from "
                    f"Tx {tx and tx.id} (LSN: {tx and tx.commit_lsn})"
                )
                with self.session.no_autoflush:
                    nested_trans = self.session.begin_nested()
                    try:
                        for op in action.prepare(session=self.session):
                            if op.type == OperationType.INSERT:
                                row = op.as_row_dict()
                                self.logger.debug(f"INSERT {op.model}: {row}")
                                self.session.execute(
                                    sa.insert(op.model),
                                    [row],
                                    **exec_kwargs,
                                )
                            elif op.type == OperationType.DELETE:
                                self.logger.debug(f"DELETE {op.model}: {op.data}")
                                self.session.execute(
                                    sa.delete(op.model).where(*op.pk_clauses),
                                    **exec_kwargs,
                                )
                            elif op.type == OperationType.UPDATE:
                                row = op.as_row_dict()
                                self.logger.debug(f"UPDATE {op.model}: {op.data}")
                                self.session.execute(
                                    sa.update(op.model),
                                    [row],
                                    **exec_kwargs,
                                )
                            self.session.flush()
                            self._stats.ops += 1
                        nested_trans.commit()
                        self._stats.tx += 1
                    except Exception:
                        self.logger.exception(
                            f"Could not load {action.data} ({action.name})",
                            exc_info=True,
                        )
                        self.tx_logger.exception(
                            "Failed processing transaction",
                            extra={"tx": action.data.tx},
                            exc_info=True,
                        )
                        nested_trans.rollback()
                        if self.raise_on_db_error:
                            raise
                self.logger.info(
                    f"END  | [{action.name}] from "
                    f"Tx {tx and tx.id} (LSN: {tx and tx.commit_lsn})"
                )
                self.logger.info(self._stats)
        except Exception:
            self.logger.exception("Transactions load failed", exc_info=True)
            self.tx_logger.exception("Failed transaction", exc_info=True)
            if self.raise_on_db_error:
                # NOTE: the "finally" block below will run before this "raise"
                raise
        finally:
            if self.dry and outer_trans:
                outer_trans.rollback()

    def run(self, entries, cleanup=False):
        """Load entries."""
        self._load(entries)
