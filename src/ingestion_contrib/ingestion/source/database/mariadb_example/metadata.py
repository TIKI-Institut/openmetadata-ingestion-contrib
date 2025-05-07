from typing import Optional

from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.database.mariadb.metadata import MariadbSource
from metadata.ingestion.source.database.mysql.utils import col_type_map, parse_column
from metadata.utils.logger import ingestion_logger
from sqlalchemy.dialects.mysql.base import ischema_names
from sqlalchemy.dialects.mysql.reflection import MySQLTableDefinitionParser

ischema_names.update(col_type_map)

MySQLTableDefinitionParser._parse_column = (  # pylint: disable=protected-access
    parse_column
)

logger = ingestion_logger()


class MariadbCustomSource(MariadbSource):
    """
    Implements the necessary methods to extract
    Database metadata from Hive Source
    """

    @classmethod
    def create(
            cls, config_dict, metadata: OpenMetadata, pipeline_name: Optional[str] = None
    ):
        logger.info("MariadbCustomSource.create was called")
        return MariadbSource.create(config_dict, metadata, pipeline_name)
