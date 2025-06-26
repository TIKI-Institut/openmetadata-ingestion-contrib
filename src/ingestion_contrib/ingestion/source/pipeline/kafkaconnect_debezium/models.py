from metadata.generated.schema.entity.data.container import Container
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.table import Table
from metadata.ingestion.source.pipeline.kafkaconnect.models import KafkaConnectDatasetDetails


class KafkaConnectDatasetDetailsExt(KafkaConnectDatasetDetails):
    @classmethod
    def from_parent(cls, a: KafkaConnectDatasetDetails):
        # Create new b_obj
        b_obj = cls()
        # Copy all values of A to B
        # It does not have any problem since they have common template
        for key, value in a.__dict__.items():
            b_obj.__dict__[key] = value
        return b_obj

    @property
    def dataset_type(self):
        if self.table:
            return Table
        if self.database:
            return Database
        if self.container_name:
            return Container
        return None
