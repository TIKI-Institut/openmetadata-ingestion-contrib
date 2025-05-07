from metadata.utils.service_spec import BaseSpec

ServiceSpec = BaseSpec(
    metadata_source_class="ingestion_contrib.ingestion.source.messaging.kafka_example.metadata.KafkaCustomSource")
