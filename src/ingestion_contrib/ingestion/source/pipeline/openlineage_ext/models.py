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
Openlineage Source Model module
"""

from dataclasses import dataclass
from typing import Optional

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
