# -*- coding: utf-8 -*-
#
# Copyright (C) 2023 CERN.
#
# Invenio-RDM-Migrator is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Invenio RDM migration drafts row load module."""

from dataclasses import dataclass
from datetime import datetime

from ....actions import LoadAction, LoadData
from ....load.ids import generate_pk, generate_uuid
from ....load.postgresql.transactions.operations import Operation, OperationType
from ....state import STATE
from ...models.files import FilesBucket
from ...models.pids import PersistentIdentifier
from ...models.records import RDMDraftMetadata, RDMParentMetadata, RDMVersionState
from ...records.table_generators.references import (
    CommunitiesReferencesMixin,
    PIDsReferencesMixin,
)
from .parents import generate_parent_ops


@dataclass
class RDMDraftCreateData(LoadData):
    """Draft create action data."""

    parent_pid: dict
    parent: dict
    draft_pid: dict
    draft: dict
    draft_bucket: dict


class DraftCreateAction(LoadAction, CommunitiesReferencesMixin, PIDsReferencesMixin):
    """RDM draft creation."""

    name = "create-draft"
    data_cls = RDMDraftCreateData

    pks = [
        # not both parent.json.pid and draft.json.pid come filled in from the
        # transform action to match parent_pid and draft_pid data.
        ("draft_pid", "id", generate_pk),
        ("draft", "id", generate_uuid),
        ("parent_pid", "id", generate_pk),
        ("parent", "id", generate_uuid),
    ]

    def _generate_rows(self, **kwargs):
        """Generates rows for a new draft."""
        yield from self._generate_pid_rows(**kwargs)
        yield from self._generate_bucket_rows(**kwargs)
        yield from self._generate_draft_rows(**kwargs)

    def _generate_pid_rows(self, **kwargs):
        """Generates rows for a new draft."""
        pid = self.data.draft_pid
        if pid["pid_type"] != "depid":
            # note would raise an exception if it exists
            STATE.PIDS.add(
                pid["pid_value"],  # recid
                {
                    "id": pid["id"],
                    "pid_type": pid["pid_type"],
                    "status": pid["status"],
                    "obj_type": pid["object_type"],
                    "created": pid["created"],
                },
            )
            yield Operation(OperationType.INSERT, PersistentIdentifier, pid)

    def _generate_bucket_rows(self, **kwargs):
        """Generates rows for a new draft."""
        yield Operation(OperationType.INSERT, FilesBucket, self.data.draft_bucket)

    def _generate_draft_rows(self, **kwargs):
        """Generates rows for a new draft."""
        now = datetime.utcnow().isoformat()

        draft = self.data.draft
        parent = self.data.parent

        forked_published = STATE.RECORDS.get(draft["json"]["id"])
        existing_parent = STATE.PARENTS.get(parent["json"]["id"])

        # parent id
        #  a) draft of a published record, parent id = parent id of published
        #  b) new version, parent id = parent id of the previous version
        #  c) draft of a new record, parent id = given by pk func

        # both values should be equal at first, the cannot be set in the transform step
        # parent.id is calculated in the pks step
        draft["parent_id"] = parent["id"]
        if not existing_parent:  # case c
            STATE.PARENTS.add(
                parent["json"]["id"],  # recid
                {"id": parent["id"], "next_draft_id": draft["id"]},
            )
            # drafts have a parent on save
            # on the other hand there is no community parent/request
            yield from generate_parent_ops(parent, self.data.parent_pid)

        else:  # case a and b
            parent["id"] = existing_parent["id"]
            draft["parent_id"] = existing_parent["id"]  # keep metadata consistent
            if not forked_published:
                # it can only happen once
                assert not existing_parent.get("next_draft_id")
                STATE.PARENTS.update(
                    parent["json"]["id"],
                    {"next_draft_id": draft["id"]},
                )
            else:
                # state parent  and an existing record must match
                assert parent["id"] == forked_published["parent_id"]

        if not forked_published:
            # recid must have been created by a previous action in the same tx group
            draft_pid = STATE.PIDS.get(draft["json"]["id"])
            assert draft_pid

            # update to add object_uuid
            # could avoid this operation but it is clearer on when and why this happens
            yield Operation(
                OperationType.UPDATE,
                PersistentIdentifier,
                dict(
                    id=draft_pid["id"],  # pk
                    pid_type=draft_pid["pid_type"],  # in drafts are recid
                    pid_value=draft["json"]["id"],
                    status=draft_pid["status"],
                    object_type="rec",  # hardcoded since the state has the initial one
                    object_uuid=draft["id"],
                    created=draft_pid["created"],
                    updated=now,
                ),
            )

        draft_id = forked_published.get("id") or draft["id"]

        STATE.BUCKETS.add(draft["bucket_id"], {"draft_id": draft_id})
        yield Operation(
            OperationType.INSERT,
            RDMDraftMetadata,
            dict(
                id=draft_id,
                json=draft["json"],
                created=draft["created"],
                updated=draft["updated"],
                version_id=draft["version_id"],
                index=forked_published.get("index") or draft["index"],
                bucket_id=draft["bucket_id"],
                parent_id=parent["id"],
                expires_at=draft["expires_at"],
                fork_version_id=forked_published.get("fork_version_id")
                or draft["fork_version_id"],
            ),
        )

        # this query can be avoided by keeping a consistent view across this method
        existing_parent = STATE.PARENTS.get(parent["json"]["id"])
        version_op = OperationType.UPDATE if forked_published else OperationType.INSERT
        yield Operation(
            version_op,
            RDMVersionState,
            dict(
                latest_index=existing_parent["latest_index"],
                parent_id=existing_parent["id"],
                latest_id=existing_parent["latest_id"],
                next_draft_id=existing_parent["next_draft_id"],
            ),
        )

    def _resolve_references(self, **kwargs):
        """Resolve references e.g communities slug names."""
        # resolve parent communities slug
        parent = self.data.parent
        communities = parent["json"].get("communities")
        if communities:
            self.resolve_communities(communities)
        self.resolve_draft_pids(self.data.draft)


@dataclass
class RDMDraftEditData(LoadData):
    """Draft edit action data."""

    draft: dict
    parent: dict


class DraftEditAction(LoadAction, CommunitiesReferencesMixin, PIDsReferencesMixin):
    """RDM draft edit/update."""

    name = "edit-draft"
    data_cls = RDMDraftEditData

    def _generate_rows(self, **kwargs):
        """Generates rows for a new draft."""
        draft = self.data.draft
        parent = self.data.parent

        assert parent["id"]  # make sure we have an id to update on
        # data and model keys/fields are the same
        yield Operation(OperationType.UPDATE, RDMParentMetadata, parent)

        forked_published = STATE.RECORDS.get(draft["json"]["id"])
        draft_id = forked_published.get("id") or draft["id"]
        assert draft_id
        draft["id"] = draft_id

        # the index and forked_version_id have been properly set from cached when the draft
        # was created, therefore now they are only passed (from the draft) if given in the
        # partial data
        # the parent_id cannot change unless is a support operation (e.g. merge) which
        # will not happen during migration
        # therefore all fields come from the draft and match data/model keys/fields
        yield Operation(OperationType.UPDATE, RDMDraftMetadata, draft)

    def _resolve_references(self, **kwargs):
        """Resolve references e.g communities slug names."""
        # resolve parent communities slug
        parent = self.data.parent
        communities = parent["json"].get("communities")
        if communities:
            self.resolve_communities(communities)
        self.resolve_draft_pids(self.data.draft)

        # resolve parent and draft uuid from versioning table
        state_parent = STATE.PARENTS.get(parent["json"]["id"])
        self.data.draft["id"] = state_parent["next_draft_id"]
        self.data.parent["id"] = state_parent["id"]