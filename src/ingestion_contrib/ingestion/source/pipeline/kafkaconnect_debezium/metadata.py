import traceback
from typing import Iterable, Optional

from ingestion_contrib.ingestion.source.pipeline.kafkaconnect_debezium.models import KafkaConnectDatasetDetailsExt
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.data.container import Container
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.pipeline import (
    Pipeline,
)
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.data.topic import Topic
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.type.entityLineage import EntitiesEdge, LineageDetails
from metadata.generated.schema.type.entityLineage import Source as LineageSource
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.api.models import Either
from metadata.ingestion.ometa.ometa_api import OpenMetadata, T
from metadata.ingestion.source.pipeline.kafkaconnect import client
from metadata.ingestion.source.pipeline.kafkaconnect.metadata import KafkaconnectSource
from metadata.ingestion.source.pipeline.kafkaconnect.models import (
    KafkaConnectPipelineDetails,
)
from metadata.utils import fqn
from metadata.utils.constants import ENTITY_REFERENCE_TYPE_MAP
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()

# Monkey Patch
# Adds keys for debezium connectors
client.SUPPORTED_DATASETS['database'].append("database.names")


class KafkaConnectDebeziumSource(KafkaconnectSource):
    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        logger.info("Custom KafkaConnectDebeziumSource was instantiated")
        super().__init__(config, metadata)

    def yield_pipeline_lineage_details(
            self, pipeline_details: KafkaConnectPipelineDetails
    ) -> Iterable[Either[AddLineageRequest]]:
        """
        Get lineage between pipeline and data sources
        """
        try:
            if not self.service_connection.messagingServiceName:
                logger.debug("Kafka messagingServiceName not found")
                return None

            pipeline_fqn = fqn.build(
                metadata=self.metadata,
                entity_type=Pipeline,
                service_name=self.context.get().pipeline_service,
                pipeline_name=self.context.get().pipeline,
            )

            pipeline_entity = self.metadata.get_by_name(
                entity=Pipeline, fqn=pipeline_fqn
            )

            lineage_details = LineageDetails(
                pipeline=EntityReference(id=pipeline_entity.id.root, type="pipeline"),
                source=LineageSource.PipelineLineage,
            )

            dataset_entity = self.get_dataset_entity(pipeline_details=pipeline_details)

            # if we have a related database we have to search and relate child tables later
            if isinstance(dataset_entity, Database):
                dataset_entities = list(self.metadata.list_all_entities(
                    entity=Table,
                    params={"database": dataset_entity.fullyQualifiedName.root}
                ))

                dataset_entity = None

                conn_config = self.client.get_connector_config(connector=pipeline_details.name)
                topic_prefix = conn_config['topic.prefix']

            for topic in pipeline_details.topics or []:
                topic_fqn = fqn.build(
                    metadata=self.metadata,
                    entity_type=Topic,
                    service_name=self.service_connection.messagingServiceName,
                    topic_name=str(topic.name),
                )

                topic_entity = self.metadata.get_by_name(entity=Topic, fqn=topic_fqn)

                if topic_entity is None:
                    continue

                if dataset_entities:
                    topic_without_prefix = topic.name.removeprefix(topic_prefix)
                    dataset_entity = next(entity for entity in dataset_entities
                                          if entity.fullyQualifiedName.root.endswith(topic_without_prefix))

                if dataset_entity is None:
                    continue

                if pipeline_details.conn_type.lower() == "sink":
                    from_entity, to_entity = topic_entity, dataset_entity
                else:
                    from_entity, to_entity = dataset_entity, topic_entity

                yield Either(
                    right=AddLineageRequest(
                        edge=EntitiesEdge(
                            fromEntity=EntityReference(
                                id=from_entity.id,
                                type=ENTITY_REFERENCE_TYPE_MAP[
                                    type(from_entity).__name__
                                ],
                            ),
                            toEntity=EntityReference(
                                id=to_entity.id,
                                type=ENTITY_REFERENCE_TYPE_MAP[
                                    type(to_entity).__name__
                                ],
                            ),
                            lineageDetails=lineage_details,
                        )
                    )
                )
        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name=pipeline_details.name,
                    error=f"Wild error ingesting pipeline lineage {pipeline_details} - {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )

        return None

    def get_dataset_entity(
            self, pipeline_details: KafkaConnectPipelineDetails
    ) -> Optional[T]:
        """
        Get lineage dataset entity
        """
        try:
            # Check if the kafka connect source handles multiple tables
            dataset_details = KafkaConnectDatasetDetailsExt.from_parent(self.client.get_connector_dataset_info(
                connector=pipeline_details.name
            ))

            if dataset_details:
                if (
                        (issubclass(dataset_details.dataset_type, Table) or issubclass(dataset_details.dataset_type,
                                                                                       Database))
                        and self.source_config.lineageInformation.dbServiceNames
                ):
                    for dbservicename in (
                            self.source_config.lineageInformation.dbServiceNames or []
                    ):
                        if issubclass(dataset_details.dataset_type, Table):
                            dataset_entity_fqn = fqn.build(
                                metadata=self.metadata,
                                entity_type=dataset_details.dataset_type,
                                table_name=dataset_details.table,
                                database_name=dataset_details.database,
                                service_name=dbservicename,
                            )
                        else:
                            dataset_entity_fqn = fqn.build(
                                metadata=self.metadata,
                                entity_type=dataset_details.dataset_type,
                                database_name=dataset_details.database,
                                service_name=dbservicename,
                            )

                        dataset_entity = self.metadata.get_by_name(
                            entity=dataset_details.dataset_type,
                            fqn=dataset_entity_fqn,
                        )

                        if dataset_entity:
                            return dataset_entity

                if (
                        issubclass(dataset_details.dataset_type, Container)
                        and self.source_config.lineageInformation.storageServiceNames
                ):
                    for storageservicename in (
                            self.source_config.lineageInformation.storageServiceNames or []
                    ):
                        dataset_entity = self.metadata.get_by_name(
                            entity=dataset_details.dataset_type,
                            fqn=fqn.build(
                                metadata=self.metadata,
                                entity_type=dataset_details.dataset_type,
                                container_name=dataset_details.container_name,
                                service_name=storageservicename,
                            ),
                        )

                        if dataset_entity:
                            return dataset_entity


        except Exception as exc:
            logger.debug(traceback.format_exc())
            logger.warning(f"Unable to get dataset entity {exc}")

        return None
