from typing import Optional

from metadata.generated.schema.entity.services.connections.database.icebergConnection import IcebergConnection
from metadata.ingestion.api.step import Step
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.database.lineage_source import LineageSource

from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()

from ingestion_contrib.ingestion.source.database.iceberg_custom.query_parser import IcebergQueryParserSource


class IcebergLineageSource(IcebergQueryParserSource, LineageSource):

    @classmethod
    def create(cls, config_dict: dict, metadata: OpenMetadata, pipeline_name: Optional[str] = None) -> "Step":
        config: WorkflowSource = WorkflowSource.model_validate(config_dict)
        connection: IcebergConnection = config.serviceConnection.root.config
        if not isinstance(connection, IcebergConnection):
            raise InvalidSourceException(
                f"Expected IcebergConnection, got {connection}"
            )
        return cls(config, metadata)
