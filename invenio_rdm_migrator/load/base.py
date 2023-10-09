# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# Invenio-RDM-Migrator is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Invenio RDM migration load interfaces."""


from abc import ABC, abstractmethod

from ..logging import Logger


class Load(ABC):
    """Base class for data loading."""

    def __init__(self) -> None:
        self._logger = None
        super().__init__()

    @property
    def logger(self):
        """Migration module logger."""
        if self._logger is None:
            self._logger = Logger.get_logger()
        return self._logger

    def _prepare(self, *args, **kwargs):  # pragma: no cover
        """Prepare data for loading."""
        pass

    @abstractmethod
    def _load(self, *args, **kwargs):  # pragma: no cover
        """load data."""
        pass

    @abstractmethod
    def _cleanup(self):  # pragma: no cover
        """Cleanup data after loading."""
        pass

    def _validate(self, *args, **kwargs):
        """Validate data before loading."""
        # It should default to False or be abstract to force implementation.
        # However, being pragmatic and due to time/resources constraints this validation
        # is done live in the application after migration (e.g. system field relations)
        return True

    def run(self, entries, cleanup=False):
        """Load entries."""
        for entry in entries:
            if self._validate(entry):
                self._prepare(entry)
                self._load(entry)

        if cleanup:
            self._cleanup()
