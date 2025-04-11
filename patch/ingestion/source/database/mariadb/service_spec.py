from metadata.utils.service_spec.default import DefaultDatabaseSpec

ServiceSpec = DefaultDatabaseSpec(
    metadata_source_class="ingestion_contrib.mariadb.metadata.MariadbCustomSource",
    lineage_source_class="metadata.ingestion.source.database.mariadb.lineage.MariadbLineageSource",
    profiler_class="metadata.profiler.interface.sqlalchemy.mariadb.profiler_interface.MariaDBProfilerInterface",
)
