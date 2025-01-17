# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# Invenio-RDM-Migrator is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Invenio RDM migration record load module."""

from ...load import PostgreSQLCopyLoad
from .table_generator import RDMRecordTableLoad, RDMVersionStateComputedTable


class RDMRecordCopyLoad(PostgreSQLCopyLoad):  # TODO: abstract SQL from PostgreSQL?
    """PostgreSQL COPY load."""

    def __init__(self, db_uri, output_path):
        """Constructor."""
        # used to keep track of what Parent IDs we've already inserted in the PIDs table.
        # {
        #     '<parent_pid>': {
        #         'id': <generated_parent_uuid>,
        #         'version': {
        #             'latest_index': 'record_index',
        #             'latest_id': 'record id',
        #         }
        # }
        self.parent_cache = {}
        super().__init__(
            db_uri=db_uri,
            output_path=output_path,
            table_loads=[
                RDMRecordTableLoad(self.parent_cache),
                RDMVersionStateComputedTable(self.parent_cache),
            ],
        )

    def _validate(self):
        """Validate data before loading."""
        pass
