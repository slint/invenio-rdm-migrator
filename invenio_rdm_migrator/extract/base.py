# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 CERN.
#
# Invenio-RDM-Migrator is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Invenio RDM migration extract interfaces."""


from abc import ABC, abstractmethod

from ..logging import Logger


class Extract(ABC):
    """Base class for data extraction."""

    def __init__(self, *args, **kwargs) -> None:
        self._logger = None
        super().__init__()

    @property
    def logger(self):
        """Migration module logger."""
        if self._logger is None:
            self._logger = Logger.get_logger()
        return self._logger

    @abstractmethod
    def run(self):  # pragma: no cover
        """Yield one element at a time."""
        pass
