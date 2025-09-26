import json
import traceback
from datetime import datetime
from itertools import product
from typing import Any, Dict, Iterable, List, Optional

from metadata.generated.schema.api.data.createPipeline import CreatePipelineRequest
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.data.pipeline import Pipeline, PipelineStatus
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.data.topic import Topic
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.type.basic import Timestamp
from metadata.generated.schema.type.entityLineage import (
    EntitiesEdge,
    LineageDetails,
    Source,
)
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.models.pipeline_status import OMetaPipelineStatus
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.pipeline.openlineage.metadata import OpenlineageSource
from metadata.ingestion.source.pipeline.openlineage.models import (
    OpenLineageEvent,
    TableDetails,
)
from metadata.utils import fqn
from metadata.utils.logger import ingestion_logger

from ingestion_contrib.ingestion.source.pipeline.openlineage_ext.models import (
    LineageEdge,
    LineageNode,
    TopicDetails,
    DataSourceFacet, JobTypeFacet, SqlFacet, EventType, OpenLineageEventExt
)
from ingestion_contrib.ingestion.source.pipeline.openlineage_ext.utils import message_to_open_lineage_event

logger = ingestion_logger()


class OpenlineageExtSource(OpenlineageSource):
    """
    Extends the original OpenlineageSource and changes following behaviour:
        - can create lineages to Kafka Topics
        - only relate existing entities -> now entity creation if non existent
        - processes every OpenLineage event (original only COMPLETE)
    """

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        logger.info("Custom OpenlineageExtSource was instantiated")
        super().__init__(config, metadata)

    @classmethod
    def _is_kafka_topic(cls, data: Dict) -> bool:
        if data.get("namespace").startswith("kafka://"):
            return True

        return False

    @classmethod
    def _get_jobtype_facet(cls, job_data: Dict) -> Optional[JobTypeFacet]:
        facets = job_data.get("facets") or {}

        if facets.get("jobType"):
            jobtype_facet = facets.get("jobType")
            return JobTypeFacet(processingType=jobtype_facet.get("processingType"),
                                integration=jobtype_facet.get("integration"),
                                jobType=jobtype_facet.get("jobType", None))

        return None

    @classmethod
    def _get_sql_facet(cls, job_data: Dict) -> Optional[SqlFacet]:
        facets = job_data.get("facets") or {}

        if facets.get("sql"):
            sql_facet = facets.get("sql")
            return SqlFacet(query=sql_facet.get("query"), dialect=sql_facet.get("dialect", None))

        return None

    @classmethod
    def _get_topic_details(cls, data: Dict) -> Optional[TopicDetails]:
        facets = data.get("facets") or {}
        symlinks = facets.get("symlinks", {}).get("identifiers") or []
        name = symlinks[0]["name"] if len(symlinks) > 1 else data["name"]
        namespace = data["namespace"]

        data_source = None

        if facets.get("dataSource"):
            data_source = DataSourceFacet(name=facets.get("dataSource").get("name"),
                                          uri=facets.get("dataSource").get("uri"))

        return TopicDetails(namespace=namespace, name=name, data_source=data_source) if name else None

    def _find_existing_topic(self, topic_details: TopicDetails) -> Optional[Topic]:
        services = self.get_db_service_names()

        for db_service in services:
            result = self.metadata.get_by_name(entity=Topic, fqn=fqn.build(
                metadata=self.metadata,
                entity_type=Topic,
                service_name=db_service,
                topic_name=topic_details.name))

            if result:
                return result

        return None

    def _find_existing_table(self, table_details: TableDetails) -> Optional[Table]:
        services = self.get_db_service_names()

        for db_service in services:
            result = self.metadata.get_by_name(entity=Table, fqn=fqn.build(
                metadata=self.metadata,
                entity_type=Table,
                service_name=db_service,
                database_name=table_details.database,
                schema_name=table_details.schema,
                table_name=table_details.name,
            ))

            if result:
                return result

        return None

    @classmethod
    def _get_table_details(cls, data: Dict) -> TableDetails:
        """
        extracts table entity schema and name from input/output entry collected from Open Lineage.

        :param data: single entry from inputs/outputs objects
        :return: TableDetails object with schema and name
        """
        facets = data.get("facets") or {}
        symlinks = facets.get("symlinks", {}).get("identifiers", [])

        # for some OL events name can be extracted from dataset facet but symlinks is preferred so - if present - we
        # use it instead
        if len(symlinks) > 0:
            try:
                # @todo verify if table can have multiple identifiers pointing at it
                name = symlinks[0]["name"]
            except (KeyError, IndexError):
                raise ValueError(
                    "input table name cannot be retrieved from symlinks.identifiers facet."
                )
        else:
            try:
                name = data["name"]
            except KeyError:
                raise ValueError(
                    "input table name cannot be retrieved from name attribute."
                )

        name_parts = name.split(".")

        if len(name_parts) < 2:
            raise ValueError(
                f"input table name should be of 'schema.table' format! Received: {name}"
            )

        database = None

        if facets.get("catalog"):
            catalog = facets.get("catalog")
            if catalog.get("framework") == "iceberg" and catalog.get("type") == "rest":
                database = catalog.get("warehouseUri")

        # we take last two elements to explicitly collect schema and table names
        # in BigQuery Open Lineage events name_parts would be list of 3 elements as first one is GCP Project ID
        # however, concept of GCP Project ID is not represented in Open Metadata and hence - we need to skip this part
        return TableDetails(name=name_parts[-1], schema=name_parts[-2], database=database)

    def yield_pipeline_lineage_details(
            self, pipeline_details: OpenLineageEvent
    ) -> Iterable[Either[AddLineageRequest]]:
        inputs, outputs = pipeline_details.inputs, pipeline_details.outputs

        input_edges: List[LineageNode] = []
        output_edges: List[LineageNode] = []

        for spec in [(inputs, input_edges), (outputs, output_edges)]:
            io_entities, io_edges = spec

            for io_entity in io_entities:
                if OpenlineageExtSource._is_kafka_topic(io_entity):
                    topic_details = OpenlineageExtSource._get_topic_details(io_entity)

                    topic = self._find_existing_topic(topic_details)

                    if topic:
                        io_edges.append(
                            LineageNode(
                                fqn=str(topic.fullyQualifiedName),
                                uuid=topic.id.root.urn,
                                node_type="topic"
                            )
                        )
                else:
                    table_details = OpenlineageExtSource._get_table_details(io_entity)

                    table = self._find_existing_table(table_details)

                    if table:
                        io_edges.append(
                            LineageNode(
                                fqn=str(table.fullyQualifiedName),
                                uuid=table.id.root.urn,
                            )
                        )

        edges = [
            LineageEdge(from_node=n[0], to_node=n[1])
            for n in product(input_edges, output_edges)
        ]

        # TODO: check impl
        # column_lineage = self._get_column_lineage(inputs, outputs)
        column_lineage = {}

        pipeline_fqn = fqn.build(
            metadata=self.metadata,
            entity_type=Pipeline,
            service_name=self.context.get().pipeline_service,
            pipeline_name=self.context.get().pipeline,
        )

        pipeline_entity = self.metadata.get_by_name(entity=Pipeline, fqn=pipeline_fqn)

        jobtype_facet = OpenlineageExtSource._get_jobtype_facet(pipeline_details.job)
        sql_facet = OpenlineageExtSource._get_sql_facet(pipeline_details.job)

        for edge in edges:
            yield Either(
                right=AddLineageRequest(
                    edge=EntitiesEdge(
                        fromEntity=EntityReference(
                            id=edge.from_node.uuid, type=edge.from_node.node_type
                        ),
                        toEntity=EntityReference(
                            id=edge.to_node.uuid, type=edge.to_node.node_type
                        ),
                        lineageDetails=LineageDetails(
                            pipeline=EntityReference(
                                id=pipeline_entity.id.root,
                                type="pipeline",
                            ),
                            sqlQuery=sql_facet.query if sql_facet else None,
                            description=f"{str(jobtype_facet) if jobtype_facet else "Lineage extracted from OpenLineage"} job: {pipeline_details.job['name']}",
                            source=Source.OpenLineage,
                            columnsLineage=column_lineage.get(
                                edge.to_node.fqn, {}
                            ).get(edge.from_node.fqn, []),
                        ),
                    ),
                )
            )

    def yield_pipeline(
            self, pipeline_details: OpenLineageEvent
    ) -> Iterable[Either[CreatePipelineRequest]]:
        pipeline_name = self.get_pipeline_name(pipeline_details)
        try:
            description = f"""```json
            {json.dumps(pipeline_details.run_facet, indent=4).strip()}```"""
            request = CreatePipelineRequest(
                name=pipeline_name,
                service=self.context.get().pipeline_service,
                description=description,
                tasks=[]
            )

            yield Either(right=request)
            self.register_record(pipeline_request=request)
        except ValueError:
            yield Either(
                left=StackTraceError(
                    name=pipeline_name,
                    message="Failed to collect metadata required for pipeline creation.",
                ),
                stackTrace=traceback.format_exc(),
            )

    def yield_pipeline_status(
            self, pipeline_details: OpenLineageEventExt
    ) -> Iterable[Either[OMetaPipelineStatus]]:
        """
        Get Pipeline Status
        """
        pipeline_name = self.get_pipeline_name(pipeline_details)

        try:
            pipeline_status = PipelineStatus(
                executionStatus=EventType.to_om_status(EventType(pipeline_details.event_type)),
                timestamp=Timestamp(int(datetime.fromisoformat(pipeline_details.event_time).timestamp() * 1000)),
                taskStatus=[]
            )

            pipeline_fqn = fqn.build(
                metadata=self.metadata,
                entity_type=Pipeline,
                service_name=self.context.get().pipeline_service,
                pipeline_name=self.context.get().pipeline,
            )

            yield Either(
                right=OMetaPipelineStatus(
                    pipeline_fqn=pipeline_fqn,
                    pipeline_status=pipeline_status,
                )
            )

        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name=pipeline_name,
                    error=f"Wild error ingesting pipeline status {pipeline_name} - {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )

    def get_pipelines_list(self) -> Optional[List[Any]]:
        """Get List of all pipelines"""
        try:
            consumer = self.client
            session_active = True
            empty_msg_cnt = 0
            pool_timeout = self.service_connection.poolTimeout
            while session_active:
                message = consumer.poll(timeout=pool_timeout)
                if message is None:
                    logger.debug("no new messages")
                    empty_msg_cnt += 1
                    if (
                            empty_msg_cnt * pool_timeout
                            > self.service_connection.sessionTimeout
                    ):
                        # There is no new messages, timeout is passed
                        session_active = False
                else:
                    logger.debug(f"new message {message.value()}")
                    empty_msg_cnt = 0
                    try:
                        result = message_to_open_lineage_event(
                            json.loads(message.value())
                        )

                        if EventType(result.event_type) in [EventType.COMPLETE, EventType.RUNNING, EventType.FAIL]:
                            yield result
                    except Exception as e:
                        logger.debug(e)

        except Exception as e:
            traceback.print_exc()

            raise InvalidSourceException(f"Failed to read from Kafka: {str(e)}")

        finally:
            # Close down consumer to commit final offsets.
            # @todo address this
            consumer.close()
