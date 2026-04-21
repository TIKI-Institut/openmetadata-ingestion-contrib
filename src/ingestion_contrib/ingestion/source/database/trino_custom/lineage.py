import traceback
from typing import Iterable, Dict, List, Optional

from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.services.ingestionPipelines.status import StackTraceError
from metadata.ingestion.api.models import Either
from metadata.ingestion.source.database.trino.lineage import TrinoLineageSource


class CustomTrinoLineageSource(TrinoLineageSource):

    def check_same_table(self, table1: Table, table2: Table) -> bool:
        """
        Method to check whether the table1 and table2 are same without considering the case
        """
        return table1.name.root.lower() == table2.name.root.lower() and {
            column.name.root.lower() for column in table1.columns
        } == {column.name.root.lower() for column in table2.columns}

    def _get_cross_database_table(
            self,
            cross_database_schema_fqn: str,
            trino_table: Table,
            cross_database_table_schema_mapping: Dict[str, Dict[str, List[Table]]],
    ) -> Optional[Table]:
        # cache cross database tables and schemas: { database_schema_fqn: { table_fqn: [ table objects ] } }
        if cross_database_schema_fqn not in cross_database_table_schema_mapping:
            cross_database_table_schema_mapping[cross_database_schema_fqn] = {}
            for cross_database_table in self.metadata.list_all_entities(
                    entity=Table, params={"databaseSchema": cross_database_schema_fqn}
            ):
                (cross_database_table_schema_mapping[cross_database_schema_fqn]
                .setdefault(cross_database_table.name.root.lower(), [])
                .append(cross_database_table))

        for cross_database_table in cross_database_table_schema_mapping[cross_database_schema_fqn].get(trino_table.name.root.lower(), []):
            if self.check_same_table(trino_table, cross_database_table):
                return cross_database_table
        return None

    def yield_cross_database_lineage(self) -> Iterable[Either[AddLineageRequest]]:
        try:
            all_cross_database_fqns = self.get_cross_database_fqn_from_service_names()
            cross_database_table_schema_mapping: Dict[str, Dict[str, List[Table]]] = {}

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
                # NOTE: Currently, tables in system-defined schemas will also be checked for lineage.
                for trino_table in trino_tables:
                    for cross_database_fqn in all_cross_database_fqns:
                        cross_database_table = self._get_cross_database_table(
                            cross_database_fqn, trino_table, cross_database_table_schema_mapping
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
