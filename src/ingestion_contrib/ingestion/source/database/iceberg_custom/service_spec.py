from metadata.utils.service_spec.default import DefaultDatabaseSpec

ServiceSpec = DefaultDatabaseSpec(
    metadata_source_class="ingestion_contrib.ingestion.source.database.iceberg_custom.metadata.CustomIcebergSource",
    connection_class="ingestion_contrib.ingestion.source.database.iceberg_custom.connection.IcebergConnectionOauthFix",
    lineage_source_class="ingestion_contrib.ingestion.source.database.iceberg_custom.lineage.IcebergLineageSource"
)
