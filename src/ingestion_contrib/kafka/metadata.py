from metadata.ingestion.source.messaging.kafka.metadata import KafkaSource
from metadata.utils.logger import ingestion_logger

from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata

logger = ingestion_logger()

class KafkaCustomSource(KafkaSource):
    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        logger.info("Custom KafkaSource was instantiated")
        super().__init__(config, metadata)