from typing import Dict

from metadata.ingestion.source.pipeline.openlineage import utils

from ingestion_contrib.ingestion.source.pipeline.openlineage_ext.models import OpenLineageEventExt


def message_to_open_lineage_event(incoming_event: Dict) -> OpenLineageEventExt:
    event = utils.message_to_open_lineage_event(incoming_event)

    return OpenLineageEventExt(event_time=incoming_event.get("eventTime"), **vars(event))
