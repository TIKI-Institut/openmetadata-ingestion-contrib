from abc import ABC

from metadata.ingestion.source.database.query_parser_source import QueryParserSource


# Needed for consistent method resolution when ingesting lineage via IcebergLineageSource
class IcebergQueryParserSource(QueryParserSource, ABC):
    pass