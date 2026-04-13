import traceback
from typing import Optional, Iterable, Tuple

import pyiceberg
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.entity.data.table import Table, TableType
from metadata.generated.schema.entity.services.ingestionPipelines.status import StackTraceError
from metadata.generated.schema.type.basic import EntityName, FullyQualifiedEntityName
from metadata.ingestion.api.models import Either
from metadata.ingestion.source.database.iceberg.helper import get_table_name_as_str
from metadata.ingestion.source.database.iceberg.metadata import IcebergSource, logger
from metadata.ingestion.source.database.iceberg.models import IcebergTable
from metadata.utils import fqn
from metadata.utils.filters import filter_by_table

from ingestion_contrib.ingestion.source.database.iceberg_view_ingestion.models import IcebergView


class CustomIcebergSource(IcebergSource):

    def get_tables_name_and_type(self) -> Optional[Iterable[Tuple[str, str]]]:
        """
        Prepares the table name to be sent to stage.
        Filtering happens here.
        """
        namespace = self.context.get().database_schema

        iceberg_tables = list(map(lambda identifier: (identifier, TableType.Regular), self.iceberg.list_tables(namespace))) \
            if self.source_config.includeTables else []
        iceberg_views = list(map(lambda identifier: (identifier, TableType.View), self.iceberg.list_views(namespace))) \
            if self.source_config.includeViews else []

        for table_identifier, table_type in iceberg_tables + iceberg_views:
            try:
                table = self.iceberg.load_table(table_identifier) if table_type == TableType.Regular else self.iceberg.load_view(table_identifier)
                # extract table name from table identifier, which does not include catalog name
                table_name = get_table_name_as_str(table_identifier)

                table_fqn = fqn.build(
                    self.metadata,
                    entity_type=Table,
                    service_name=self.context.get().database_service,
                    database_name=self.context.get().database,
                    schema_name=self.context.get().database_schema,
                    table_name=table_name,
                )
                if filter_by_table(
                        self.config.sourceConfig.config.tableFilterPattern,
                        table_fqn
                        if self.config.sourceConfig.config.useFqnForFiltering
                        else table_name,
                ):
                    self.status.filter(
                        table_fqn,
                        "Table Filtered Out",
                    )
                    continue

                self.context.get().iceberg_table = table
                yield table_name, table_type

            except pyiceberg.exceptions.NoSuchPropertyException:
                logger.warning(
                    f"Table [{table_identifier}] does not have the 'table_type' property. Skipped."
                )
                continue
            except pyiceberg.exceptions.NoSuchIcebergTableError:
                logger.warning(
                    f"Table [{table_identifier}] is not an Iceberg Table. Skipped."
                )
                continue
            except pyiceberg.exceptions.NoSuchTableError:
                logger.warning(f"Table [{table_identifier}] not Found. Skipped.")
                continue
            except pyiceberg.exceptions.NoSuchViewError:
                logger.warning(f"View [{table_identifier}] not Found. Skipped.")
                continue
            except Exception as exc:
                table_name = ".".join(table_identifier)
                self.status.failed(
                    StackTraceError(
                        name=table_name,
                        error=f"Unexpected exception to get table [{table_name}]: {exc}",
                        stackTrace=traceback.format_exc(),
                    )
                )

    def yield_table(
            self, table_name_and_type: Tuple[str, TableType]
    ) -> Iterable[Either[CreateTableRequest]]:
        """
        From topology.
        Prepare a table request and pass it to the sink.

        Also, update the self.inspector value to the current db.
        """
        table_name, table_type = table_name_and_type
        iceberg_table = self.context.get().iceberg_table
        try:
            if table_type == TableType.Regular:
                owners = self.get_owner_ref(table_name)
                table = IcebergTable.from_pyiceberg(
                    table_name, table_type, owners, iceberg_table
                )
            elif table_type == TableType.View:
                table = IcebergView.from_pyiceberg(
                    table_name, iceberg_table
                )

            table_request = CreateTableRequest(
                name=EntityName(table.name),
                tableType=table.tableType,
                description=table.description,
                owners=table.owners,
                columns=table.columns,
                tablePartition=table.tablePartition,
                databaseSchema=FullyQualifiedEntityName(
                    fqn.build(
                        metadata=self.metadata,
                        entity_type=DatabaseSchema,
                        service_name=self.context.get().database_service,
                        database_name=self.context.get().database,
                        schema_name=self.context.get().database_schema,
                    )
                ),
            )
            yield Either(right=table_request)
            self.register_record(table_request=table_request)
        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name=table_name,
                    error=f"Unexpected exception to yield table [{table_name}]: {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )
