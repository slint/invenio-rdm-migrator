# -*- coding: utf-8 -*-
#
# Copyright (C) 2023 CERN.
#
# Invenio-RDM-Migrator is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Files actions module."""

from dataclasses import dataclass
from typing import Optional

import sqlalchemy as sa

from ....actions import LoadAction, LoadData
from ....load.ids import generate_uuid
from ....load.postgresql.transactions.operations import Operation, OperationType
from ...models.files import FilesBucket, FilesInstance, FilesObjectVersion
from ...models.pids import PersistentIdentifier
from ...models.records import RDMDraftFile, RDMDraftMediaFile, RDMDraftMetadata


@dataclass
class FileUploadData(LoadData):
    """File upload action data."""

    bucket: dict
    object_version: dict
    file_instance: dict
    file_record: dict
    replaced_object_version: Optional[dict] = None


class FileUploadAction(LoadAction):
    """File upload action."""

    name = "file-upload"
    data_cls = FileUploadData
    data: FileUploadData
    pks = [("file_record", "id", generate_uuid)]

    def _generate_rows(self, session, **kwargs):
        """Generates rows for a new draft."""
        replaced_ov = self.data.replaced_object_version
        new_ov = self.data.object_version
        yield Operation(OperationType.UPDATE, FilesBucket, self.data.bucket)
        if not replaced_ov:
            replaced_ov_model = session.execute(
                sa.select(FilesObjectVersion.version_id).where(
                    FilesObjectVersion.bucket_id == self.data.bucket["id"],
                    FilesObjectVersion.key == new_ov["key"],
                    FilesObjectVersion.is_head.is_(True),
                )
            ).one_or_none()
            if replaced_ov_model:
                replaced_ov = {"version_id": replaced_ov_model.version_id}

        if replaced_ov:
            yield Operation(
                OperationType.UPDATE,
                FilesObjectVersion,
                {"version_id": replaced_ov["version_id"], "is_head": False},
            )

        yield Operation(OperationType.INSERT, FilesInstance, self.data.file_instance)
        yield Operation(OperationType.INSERT, FilesObjectVersion, new_ov)
        self.data.file_record["object_version_id"] = new_ov["version_id"]
        if replaced_ov:
            file_record_id = session.execute(
                sa.select(RDMDraftFile.id).where(
                    RDMDraftFile.object_version_id == replaced_ov["version_id"],
                )
            ).scalar_one()
            self.data.file_record["id"] = file_record_id
            yield Operation(OperationType.UPDATE, RDMDraftFile, self.data.file_record)
        else:
            # Find draft ID
            draft_id = session.execute(
                sa.select(RDMDraftMetadata.id).where(
                    RDMDraftMetadata.bucket_id == self.data.bucket["id"]
                )
            ).scalar_one()
            self.data.file_record["record_id"] = draft_id
            yield Operation(OperationType.INSERT, RDMDraftFile, self.data.file_record)


@dataclass
class FileDeleteData(LoadData):
    """File delete action data."""

    bucket: dict
    deleted_object_version: dict
    delete_marker_object_version: Optional[dict] = None


class FileDeleteAction(LoadAction):
    """File delete action."""

    name = "file-delete"
    data_cls = FileDeleteData
    data: FileDeleteData

    def _generate_rows(self, *, session, **kwargs):
        """Generates rows for deleting a draft file."""
        bucket = self.data.bucket
        deleted_ov = self.data.deleted_object_version
        delete_marker = self.data.delete_marker_object_version

        yield Operation(OperationType.UPDATE, FilesBucket, bucket)

        # We always soft delete, which means we update the old OV's is_head = False...
        deleted_ov["is_head"] = False
        yield Operation(OperationType.UPDATE, FilesObjectVersion, deleted_ov)
        # ...and insert a delete marker on top
        if not delete_marker:
            delete_marker = {
                "version_id": generate_uuid(),
                "bucket_id": bucket["id"],
                "key": deleted_ov["key"],
                "created": bucket["updated"],
                "updated": bucket["updated"],
                "is_head": True,
            }
            yield Operation(OperationType.INSERT, FilesObjectVersion, delete_marker)

        # Finally we delete the draft file
        file_record = session.scalars(
            sa.select(RDMDraftFile).where(
                RDMDraftFile.object_version_id == deleted_ov["version_id"],
                RDMDraftFile.key == deleted_ov["key"],
            )
        ).one_or_none()
        if file_record:
            yield Operation(OperationType.DELETE, RDMDraftFile, {"id": file_record.id})


@dataclass
class MediaFileUploadData(LoadData):
    """Media file upload action data."""

    bucket: dict
    object_version: dict
    file_instance: dict
    file_record: dict
    replaced_object_version: Optional[dict] = None
    pid_value: Optional[dict] = None


class MediaFileUploadAction(LoadAction):
    """Media file upload action."""

    name = "media-file-upload"
    data_cls = MediaFileUploadData
    data: MediaFileUploadData
    pks = [("file_record", "id", generate_uuid)]

    def _generate_rows(self, session, **kwargs):
        """Generates rows for a new draft."""
        replaced_ov = self.data.replaced_object_version
        new_ov = self.data.object_version

        # if there's a pid passed, this is a new media bucket...
        if self.data.pid_value:
            # create it...
            yield Operation(OperationType.INSERT, FilesBucket, self.data.bucket)
            # ...and add to the draft/record
            draft_id = session.scalar(
                sa.select(PersistentIdentifier.object_uuid).where(
                    PersistentIdentifier.pid_type == "recid",
                    PersistentIdentifier.pid_value == self.data.pid_value,
                )
            )
            assert draft_id
            yield Operation(
                OperationType.UPDATE,
                RDMDraftMetadata,
                {"id": draft_id, "media_bucket_id": self.data.bucket["id"]},
            )
        else:
            yield Operation(OperationType.UPDATE, FilesBucket, self.data.bucket)

        if not replaced_ov:
            replaced_ov_model = session.execute(
                sa.select(FilesObjectVersion.version_id).where(
                    FilesObjectVersion.bucket_id == self.data.bucket["id"],
                    FilesObjectVersion.key == new_ov["key"],
                    FilesObjectVersion.is_head.is_(True),
                )
            ).one_or_none()
            if replaced_ov_model:
                replaced_ov = {"version_id": replaced_ov_model.version_id}

        if replaced_ov:
            yield Operation(
                OperationType.UPDATE,
                FilesObjectVersion,
                {"version_id": replaced_ov["version_id"], "is_head": False},
            )

        yield Operation(OperationType.INSERT, FilesInstance, self.data.file_instance)
        yield Operation(OperationType.INSERT, FilesObjectVersion, new_ov)
        self.data.file_record["object_version_id"] = new_ov["version_id"]
        if replaced_ov:
            file_record_id = session.execute(
                sa.select(RDMDraftMediaFile.id).where(
                    RDMDraftMediaFile.object_version_id == replaced_ov["version_id"],
                )
            ).scalar_one()
            self.data.file_record["id"] = file_record_id
            yield Operation(
                OperationType.UPDATE,
                RDMDraftMediaFile,
                self.data.file_record,
            )
        else:
            # Find draft ID
            draft_id = session.execute(
                sa.select(RDMDraftMetadata.id).where(
                    RDMDraftMetadata.media_bucket_id == self.data.bucket["id"]
                )
            ).scalar_one()
            self.data.file_record["record_id"] = draft_id
            yield Operation(
                OperationType.INSERT,
                RDMDraftMediaFile,
                self.data.file_record,
            )


@dataclass
class MediaFileDeleteData(LoadData):
    """Media file delete action data."""

    bucket: dict
    deleted_object_version: dict
    delete_marker_object_version: Optional[dict] = None


class MediaFileDeleteAction(LoadAction):
    """Media file delete action."""

    name = "media-file-delete"
    data_cls = MediaFileDeleteData
    data: MediaFileDeleteData

    def _generate_rows(self, session, **kwargs):
        """Generates rows for a new draft."""
        # if we were to use the state for consistency checks
        # the bucket should already exist
        bucket = self.data.bucket
        deleted_ov = self.data.deleted_object_version
        delete_marker = self.data.delete_marker_object_version

        yield Operation(OperationType.UPDATE, FilesBucket, bucket)

        # We always soft delete, which means we update the old OV's is_head = False...
        deleted_ov["is_head"] = False
        yield Operation(OperationType.UPDATE, FilesObjectVersion, deleted_ov)
        # ...and insert a delete marker on top
        if not delete_marker:
            delete_marker = {
                "version_id": generate_uuid(),
                "bucket_id": bucket["id"],
                "key": deleted_ov["key"],
                "created": bucket["updated"],
                "updated": bucket["updated"],
                "is_head": True,
            }
            yield Operation(OperationType.INSERT, FilesObjectVersion, delete_marker)

        # Finally we delete the draft file
        file_record = session.scalars(
            sa.select(RDMDraftFile).where(
                RDMDraftMediaFile.object_version_id == deleted_ov["version_id"],
                RDMDraftMediaFile.key == deleted_ov["key"],
            )
        ).one_or_none()
        if file_record:
            yield Operation(
                OperationType.DELETE,
                RDMDraftMediaFile,
                {"id": file_record.id},
            )
