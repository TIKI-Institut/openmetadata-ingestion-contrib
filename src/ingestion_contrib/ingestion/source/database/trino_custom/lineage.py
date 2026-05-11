import traceback
from typing import Iterable, Dict, Optional

from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.services.ingestionPipelines.status import StackTraceError
from metadata.ingestion.api.models import Either
from metadata.ingestion.source.database.trino.lineage import TrinoLineageSource
from metadata.utils import fqn
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class CustomTrinoLineageSource(TrinoLineageSource):
    """
    Extends the original TrinoLineageSource class to build lineage between tables/views with different cases
    Fixes the Issue https://github.com/open-metadata/OpenMetadata/issues/27419 temporarily for older OM version
    Issue fixed officially via https://github.com/open-metadata/OpenMetadata/pull/27495 and should be release in OM 1.13+
    """

    def check_same_table(self, table1: Table, table2: Table) -> bool:
        """
        Method to check whether the table1 and table2 are the same without considering the case
        """
        return table1.name.root.lower() == table2.name.root.lower() and {
            column.name.root.lower() for column in table1.columns
        } == {column.name.root.lower() for column in table2.columns}

    def _get_cross_schema_fqn(self, source_schema_fqn: str) -> Optional[str]:
        """
        Get the cross schema fqn with the correct case.
        Trino tracks every object in lowercase, but the cross database services could use upper case letters.
        We use the ElasticSearch Index to do a fast case invariant lookup.
        """
        service_name, database_name, schema_name = fqn.split(source_schema_fqn)
        target_schema_fqn = fqn.search_database_schema_from_es(metadata=self.metadata,
                                                               service_name=service_name,
                                                               database_name=database_name,
                                                               schema_name=schema_name)

        return target_schema_fqn.fullyQualifiedName.root if target_schema_fqn and target_schema_fqn.fullyQualifiedName else None

    def _get_cross_database_table(
            self,
            cross_database_schema_fqn: str,
            trino_table: Table,
            cross_database_table_schema_mapping: Dict[str, Dict[str, Table]],
    ) -> Optional[Table]:
        # cache cross database tables and schemas: { database_schema_fqn: { table_fqn: table object } }
        if cross_database_schema_fqn not in cross_database_table_schema_mapping:
            cross_database_table_schema_mapping[cross_database_schema_fqn] = {}
            cross_schema_fqn = self._get_cross_schema_fqn(cross_database_schema_fqn)
            if cross_schema_fqn:
                for cross_database_table in self.metadata.list_all_entities(
                        entity=Table, params={"databaseSchema": cross_schema_fqn}
                ):
                    cross_database_table_schema_mapping[cross_database_schema_fqn][
                        cross_database_table.name.root.lower()] = cross_database_table

        cross_database_table = cross_database_table_schema_mapping[cross_database_schema_fqn].get(
            trino_table.name.root.lower())

        if cross_database_table is not None and self.check_same_table(trino_table, cross_database_table):
            return cross_database_table
        return None

    def yield_cross_database_lineage(self) -> Iterable[Either[AddLineageRequest]]:
        try:
            cross_service_names = self.source_config.crossDatabaseServiceNames
            cross_database_table_schema_mapping: Dict[str, Dict[str, Table]] = {}

            # Get all databases for the specified Trino service
            trino_databases = self.metadata.list_all_entities(
                entity=Database, params={"service": self.config.serviceName}
            )
            for trino_database in trino_databases:
                trino_database_fqn = trino_database.fullyQualifiedName.root

                # Get all tables for the specified Trino database schema
                trino_tables = self.metadata.list_all_entities(
                    entity=Table, params={"database": trino_database_fqn}
                )
                for trino_table in trino_tables:

                    if not trino_table.databaseSchema:
                        if not trino_table.fullyQualifiedName or len(
                                fqn.split(trino_table.fullyQualifiedName.root)) != 4:
                            logger.warn(f"Cannot get schema name from Trino table {trino_table.name.root}. Skipping...")
                            continue
                        trino_database_schema_fqn = ".".join(fqn.split(trino_table.fullyQualifiedName.root)[:-1])
                    else:
                        trino_database_schema_fqn = trino_table.databaseSchema.name

                    trino_service_name = trino_table.service.name if trino_table.service else fqn.split(trino_table.fullyQualifiedName)[0]
                    # NOTE: Currently, tables in system-defined schemas will also be checked for lineage.
                    for cross_service_name in cross_service_names:
                        cross_database_table = self._get_cross_database_table(
                            trino_database_schema_fqn.replace(trino_service_name, cross_service_name), trino_table,
                            cross_database_table_schema_mapping
                        )
                        if cross_database_table:
                            yield self.get_cross_database_lineage(
                                cross_database_table, trino_table
                            )
                            break
        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name=f"{self.config.serviceName} Cross Database Lineage",
                    error=(
                        "Error to yield cross database lineage details "
                        f"service name [{self.config.serviceName}]: {exc}"
                    ),
                    stackTrace=traceback.format_exc(),
                )
            )
