
from __future__ import annotations

from typing import List

import pyiceberg.view
from metadata.generated.schema.entity.data.table import (
    Column,
    TableType,
)
from metadata.generated.schema.type.basic import SqlQuery
from metadata.ingestion.source.database.iceberg.helper import IcebergColumnParser
from metadata.ingestion.source.database.iceberg.models import IcebergTable
from pydantic import BaseModel
from pyiceberg.schema import Schema
from pyiceberg.view.metadata import ViewVersion


class IcebergView(BaseModel):
    name: str
    tableType: TableType = TableType.View
    description: str
    columns: List[Column]

    @classmethod
    def from_pyiceberg(cls, name: str, view: pyiceberg.view.View) -> IcebergTable:
        return IcebergTable(
            name=name,
            tableType=TableType.View,
            description=None,
            columns=[IcebergColumnParser.parse(column) for column in IcebergView.current_schema(view).fields],
            tablePartition=None
        )

    @classmethod
    def get_current_version(cls, view: pyiceberg.view.View) -> ViewVersion:
        current_version_id = view.metadata.current_version_id
        return next(filter(lambda version: version.version_id == current_version_id, view.metadata.versions))

    @classmethod
    def current_schema(cls, view: pyiceberg.view.View) -> Schema:
        current_schema_id = IcebergView.get_current_version(view).schema_id
        return next(filter(lambda schema: schema.schema_id == current_schema_id, view.metadata.schemas))

    @classmethod
    def get_view_definition(cls, view: pyiceberg.view.View) -> SqlQuery | None:
        current_version = IcebergView.get_current_version(view)
        if current_version is None or len(current_version.representations) < 1:
            return None
        # dialect is not important here, just get the first representation
        definition = current_version.representations[0].root.sql
        return SqlQuery(root=definition)
