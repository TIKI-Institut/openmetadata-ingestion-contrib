#  Copyright 2025 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""
OpenLineage source to extract metadata from Kafka events
"""
import json
import traceback
from itertools import product
from typing import Any, Dict, Iterable, List, Optional

from metadata.generated.schema.api.data.createTopic import CreateTopicRequest
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.data.pipeline import Pipeline
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.data.topic import Topic
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.type.entityLineage import (
    EntitiesEdge,
    LineageDetails,
    Source,
)
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.pipeline.openlineage.metadata import OpenlineageSource
from metadata.ingestion.source.pipeline.openlineage.models import (
    OpenLineageEvent,
    TableDetails,
    TableFQN,
)
from metadata.ingestion.source.pipeline.openlineage.utils import (
    FQNNotFoundException,
    message_to_open_lineage_event,
)
from metadata.utils import fqn
from metadata.utils.logger import ingestion_logger

from ingestion_contrib.ingestion.source.pipeline.openlineage_ext.models import (
    LineageEdge,
    LineageNode,
    TopicDetails,
    TopicFQN
)

logger = ingestion_logger()


class OpenlineageExtSource(OpenlineageSource):
    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        logger.info("Custom OpenlineageExtSource was instantiated")
        super().__init__(config, metadata)

    @classmethod
    def _is_kafka_topic(cls, data: Dict) -> bool:
        for field in data.get("facets").get("schema").get("fields"):
            if "topic" in field.get("name", "") \
                    or "kafka" in field.get("dataSource", {}).get("uri", ""):
                return True
        return False

    @classmethod
    def _get_topic_details(cls, data: Dict) -> Optional[TopicDetails]:

        symlinks = ((data.get("facets") or {}).get("symlinks") or {}).get("identifiers") or []
        name = symlinks[0]["name"] if len(symlinks) > 1 else data["name"]

        # check for hints that this could be a kafka topic instead of a table
        # for field in data.get("facets").get("schema").get("fields"):
        #     if "topic" in field.get("name", "") \
        #             or "kafka" in field.get("dataSource", {}).get("uri", ""):

        return TopicDetails(name=name) if name else None

    def _get_topic_fqn_from_om(self, topic_details: TopicDetails) -> Optional[str]:
        result = None
        services = self.get_services()
        for service in services:
            result = fqn.build(
                metadata=self.metadata,
                entity_type=Topic,
                service_name=service.serviceName,
                topic_name=topic_details.name)
            if result:
                return result

        raise FQNNotFoundException(
            f"Topic FQN not found for topic: {topic_details} within services {services}"
        )

    @classmethod
    def _get_table_details(cls, data: Dict) -> TableDetails:
        """
        extracts table entity schema and name from input/output entry collected from Open Lineage.

        :param data: single entry from inputs/outputs objects
        :return: TableDetails object with schema and name
        """
        symlinks = ((data.get("facets") or {}).get("symlinks") or {}).get("identifiers") or []

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

        # we take last two elements to explicitly collect schema and table names
        # in BigQuery Open Lineage events name_parts would be list of 3 elements as first one is GCP Project ID
        # however, concept of GCP Project ID is not represented in Open Metadata and hence - we need to skip this part
        return TableDetails(name=name_parts[-1], schema=name_parts[-2])

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
                    # TODO relate to kafka instance
                    topic_details = OpenlineageExtSource._get_topic_details(io_entity)
                    topic_fqn = topic_details.name
                    io_edges.append(
                        LineageNode(
                            fqn=TopicFQN(value=topic_fqn),
                            uuid=self.metadata.get_by_name(Topic, topic_fqn).id,
                            node_type="topic"
                        )
                    )
                else:
                    # TODO relate to catalog instance
                    table_fqn = self._get_table_fqn(
                        OpenlineageExtSource._get_table_details(io_entity)
                    )

                    if table_fqn:
                        io_edges.append(
                            LineageNode(
                                fqn=TableFQN(value=table_fqn),
                                uuid=self.metadata.get_by_name(Table, table_fqn).id,
                            )
                        )

        edges = [
            LineageEdge(from_node=n[0], to_node=n[1])
            for n in product(input_edges, output_edges)
        ]

        column_lineage = self._get_column_lineage(inputs, outputs)

        pipeline_fqn = fqn.build(
            metadata=self.metadata,
            entity_type=Pipeline,
            service_name=self.context.get().pipeline_service,
            pipeline_name=self.context.get().pipeline,
        )

        pipeline_entity = self.metadata.get_by_name(entity=Pipeline, fqn=pipeline_fqn)
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
                            description=f"Lineage extracted from OpenLineage job: {pipeline_details.job['name']}",
                            source=Source.OpenLineage,
                            columnsLineage=column_lineage.get(
                                edge.to_node.fqn.value, {}
                            ).get(edge.from_node.fqn.value, []),
                        ),
                    ),
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
                        # result = self._filter_event_by_type(_result, EventType.COMPLETE)
                        if result:
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
