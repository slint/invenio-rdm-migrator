# -*- coding: utf-8 -*-
#
# Copyright (C) 2023 CERN.
#
# Invenio-RDM-Migrator is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Test fixtures and utilities."""

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session

from invenio_rdm_migrator.streams.models.communities import (
    Community,
    CommunityFile,
    CommunityMember,
    FeaturedCommunity,
    RDMParentCommunityMetadata,
)
from invenio_rdm_migrator.streams.models.files import (
    FilesBucket,
    FilesInstance,
    FilesObjectVersion,
)
from invenio_rdm_migrator.streams.models.github import Release, Repository, WebhookEvent
from invenio_rdm_migrator.streams.models.oai import OAISet
from invenio_rdm_migrator.streams.models.oauth import (
    RemoteAccount,
    RemoteToken,
    ServerClient,
    ServerToken,
)
from invenio_rdm_migrator.streams.models.pids import PersistentIdentifier
from invenio_rdm_migrator.streams.models.records import (
    RDMDraftFile,
    RDMDraftMediaFile,
    RDMDraftMetadata,
    RDMParentMetadata,
    RDMRecordFile,
    RDMRecordMediaFile,
    RDMRecordMetadata,
    RDMVersionState,
)
from invenio_rdm_migrator.streams.models.users import (
    LoginInformation,
    SessionActivity,
    User,
    UserIdentity,
)


@pytest.fixture(scope="session")
def db_uri():
    """Database URI."""
    return "postgresql+psycopg://invenio:invenio@localhost:5432/invenio"


@pytest.fixture(scope="session")
def engine(db_uri):
    """SQLAlchemy engine."""
    return sa.create_engine(db_uri)


@pytest.fixture(scope="function")
def session(engine):
    """SQLAlchemy session."""
    conn = engine.connect()
    transaction = conn.begin()

    session = Session(bind=conn, join_transaction_mode="create_savepoint")

    yield session

    session.close()
    transaction.rollback()
    conn.close()


@pytest.fixture(scope="session")
def database(engine):
    """Setup database.

    Scope: module

    Normally, tests should use the function-scoped :py:data:`db` fixture
    instead. This fixture takes care of creating the database/tables and
    removing the tables once tests are done.
    """
    tables = [
        FilesBucket,
        FilesInstance,
        FilesObjectVersion,
        ServerClient,
        ServerToken,
        User,
        UserIdentity,
        SessionActivity,
        LoginInformation,
        RemoteAccount,
        RemoteToken,
        RDMParentMetadata,
        Community,
        CommunityFile,
        CommunityMember,
        FeaturedCommunity,
        OAISet,
        PersistentIdentifier,
        RDMDraftMetadata,
        RDMRecordMetadata,
        RDMRecordFile,
        RDMRecordMediaFile,
        RDMDraftFile,
        RDMDraftMediaFile,
        RDMVersionState,
        RDMParentCommunityMetadata,
        WebhookEvent,
        Repository,
        Release,
    ]

    # create tables
    for model in tables:
        model.__table__.create(bind=engine, checkfirst=True)

    yield

    # remove tables
    for model in reversed(tables):
        model.__table__.drop(engine)
