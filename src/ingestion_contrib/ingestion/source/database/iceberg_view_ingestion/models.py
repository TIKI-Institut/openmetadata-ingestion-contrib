
from __future__ import annotations

from typing import List

import pyiceberg.view
from metadata.generated.schema.entity.data.table import (
    Column,
    TableType,
)
from metadata.ingestion.source.database.iceberg.helper import IcebergColumnParser
from metadata.ingestion.source.database.iceberg.models import IcebergTable
from pydantic import BaseModel
from pyiceberg.schema import Schema


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
    def current_schema(cls, view: pyiceberg.view.View) -> Schema:
        current_version_id = view.metadata.current_version_id
        current_schema_id = next(filter(lambda version: version.version_id == current_version_id, view.metadata.versions)).schema_id
        return next(filter(lambda schema: schema.schema_id == current_schema_id, view.metadata.schemas))

