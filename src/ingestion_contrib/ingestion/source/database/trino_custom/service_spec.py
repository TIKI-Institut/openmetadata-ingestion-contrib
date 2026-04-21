from metadata.ingestion.source.database.trino.connection import TrinoConnection
from metadata.ingestion.source.database.trino.metadata import TrinoSource
from metadata.ingestion.source.database.trino.usage import TrinoUsageSource
from metadata.profiler.interface.sqlalchemy.trino.profiler_interface import (
    TrinoProfilerInterface,
)
from metadata.sampler.sqlalchemy.trino.sampler import TrinoSampler
from metadata.utils.service_spec.default import DefaultDatabaseSpec

from ingestion_contrib.ingestion.source.database.trino_custom.lineage import CustomTrinoLineageSource

ServiceSpec = DefaultDatabaseSpec(
    metadata_source_class=TrinoSource,
    lineage_source_class=CustomTrinoLineageSource,
    usage_source_class=TrinoUsageSource,
    profiler_class=TrinoProfilerInterface,
    sampler_class=TrinoSampler,
    connection_class=TrinoConnection,
)
