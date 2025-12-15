from dataclasses import dataclass
from enum import Enum
from typing import Optional

from metadata.generated.schema.entity.data.pipeline import StatusType
from metadata.ingestion.source.pipeline.openlineage.models import OpenLineageEvent


@dataclass
class OpenLineageEventExt(OpenLineageEvent):
    event_time: str


@dataclass
class LineageNode:
    """
    A node being a part of Lineage information.
    """

    uuid: str
    fqn: str
    node_type: str = "table"


@dataclass
class LineageEdge:
    """
    An object describing connection of two nodes in the Lineage information.
    """

    from_node: LineageNode
    to_node: LineageNode


@dataclass
class DataSourceFacet:
    """
    DataSource facet.
    """
    name: str
    uri: str


@dataclass
class TopicDetails:
    """
    Kafka Topic information.
    """
    namespace: str
    name: str
    data_source: Optional[DataSourceFacet]


@dataclass
class JobTypeFacet:
    """
    JobType facet.
    """
    processingType: str
    integration: str
    jobType: Optional[str]

    def __str__(self):
        return f"{self.integration} {self.processingType}{' ' + self.jobType if self.jobType else ''}"


@dataclass
class SqlFacet:
    """
    Sql facet.
    """
    query: str
    dialect: Optional[str]


class EventType(str, Enum):
    """
    List of used OpenLineage event types.
    """

    COMPLETE = "COMPLETE"
    RUNNING = "RUNNING"
    START = "START"
    FAIL = "FAIL"
    ABORT = "ABORT"
    OTHER = "OTHER"

    def to_om_status(self):
        match self.value:
            case EventType.FAIL.value:
                return StatusType.Failed.value
            case _:
                return StatusType.Successful.value

