from pyiceberg.catalog import Catalog, load_rest

from metadata.generated.schema.entity.services.connections.database.iceberg.icebergCatalog import (
    IcebergCatalog,
)
from metadata.generated.schema.entity.services.connections.database.iceberg.restCatalogConnection import (
    RestCatalogConnection,
)
from metadata.ingestion.source.database.iceberg.catalog.base import IcebergCatalogBase

import json


class IcebergRestCatalog(IcebergCatalogBase):
    """Responsible for building a PyIceberg Rest Catalog."""

    @classmethod
    def get_catalog(cls, catalog: IcebergCatalog) -> Catalog:
        """Returns a Rest Catalog for the given connection and file storage.

        For more information, check the PyIceberg [docs](https://py.iceberg.apache.org/configuration/#rest-catalog)
        """
        if not isinstance(catalog.connection, RestCatalogConnection):
            raise RuntimeError(
                "'connection' is not an instance of 'RestCatalogConnection'"
            )

        if catalog.connection.credential and catalog.connection.credential.clientSecret:
            client_id = catalog.connection.credential.clientId.get_secret_value()
            client_secret = (
                catalog.connection.credential.clientSecret.get_secret_value()
            )

            credential = f"{client_id}:{client_secret}"
        else:
            credential = None

        with open("/etc/openmetadata/rest-catalog-parameters", "r") as properties_file:
            additional_rest_parameters = json.load(properties_file)

        parameters = {
            "warehouse": catalog.warehouseLocation,
            "uri": str(catalog.connection.uri),
            **additional_rest_parameters
        }

        if  credential:
            parameters["credential"] = credential

        if catalog.connection.token:
            parameters["token"] = catalog.connection.token.get_secret_value()

        if catalog.connection.fileSystem:
            parameters = {
                **parameters,
                **cls.get_fs_parameters(catalog.connection.fileSystem.type),
            }

        if catalog.connection.ssl:
            parameters = {
                **parameters,
                "ssl": {
                    "client": {
                        "cert": catalog.connection.ssl.clientCertPath,
                        "key": catalog.connection.ssl.privateKeyPath,
                    },
                    "cabundle": catalog.connection.ssl.caCertPath,
                },
            }

        if catalog.connection.sigv4:
            parameters = {
                **parameters,
                "rest.sigv4": True,
                "rest.signing_region": catalog.connection.sigv4.signingRegion,
                "rest.signing_name": catalog.connection.sigv4.signingName,
            }
        return load_rest(catalog.name, parameters)
