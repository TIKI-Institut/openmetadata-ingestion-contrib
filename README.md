# openmetadata-ingestion-contrib

Framework for customizing existing connectors of the OpenMetadata platform.
Simply it allows replacing or extending the original implementation while still using the OpenMetadata UI integration of the connectors.

Existing connectors will be shadowed by the newly developed solutions.

## Installation

Installations is similar to adding a custom connector, see [Prepare the Ingestion Image
](https://docs.open-metadata.org/latest/connectors/custom-connectors#step-4-prepare-the-ingestion-image)

Additionally a config file needs to be provided, which enables the patching / shadowing of the existing connector.

```yaml
patch-files:
   # <contrib implementation name>: 
   #  "target_dir": <target connector directory>
   #  "files": <list of files that should be copied to the destination>
   "mariadb_example":
      "target_dir": mariadb
      "files":
         - "service_spec.py"
   "kafka_example":
      "target_dir": kafka
      "files":
         - "service_spec.py"
```

Finally the patching can be executed with
```bash
patch-service-specs
```

For a whole example see [Dockerfile](./Dockerfile)

## Project structure
- `./src` contains the connector implementations like in the official [OpenMetadata repository](https://github.com/open-metadata/OpenMetadata/tree/main/ingestion/src/metadata/ingestion/source).
The package structure needs to be identical except for the last connector name. `service_spec.py` files should point the new customized implementation.
It also provides some example connectors.

- `local-openmetadata-stack` contains a Docker Compose file to deploy a Open Metadata instance locally. It should be 
started using the `Makefile` in the root directory.

## Configuration

The patch process can be configured with the [config.yaml](./config.example.yaml).
The `config.yaml` file needs to exist where the command `patch-service-specs` is executed.

`patch-files` contains a list of connectors whose files should be patched. Two keys are needed: 
   - `target_dir` determines the directory to which the files are copied
   - `files` contains the list of files that should be copied with their relative path to `target_dir` in the destination

```yaml
patch-files:
   "mariadb_example":
      "target_dir": mariadb
      "files":
         - "service_spec.py"
   "kafka_example":
      "target_dir": kafka
      "files":
         - "service_spec.py"
```
The modules names are derived from the directory in which the files are placed (for 
`src/ingestion_contrib/ingestion/source/database/mariadb_example/service_spec.py` the config entry is `mariadb_example`).

## Local Dev Stack

1. Setup a virtual environment for Python and install dependencies with 
    ```shell
    pip install -e .
    ```
2. Start local Docker containers with 
   ```shell
   make local-openmetadata-stack
   ```
3. Set up a new service and ingestion in the Open Metadata dashboard at http://localhost:8585 to test custom connectors. 
Refer to the [documentation for the various connectors](https://docs.open-metadata.org/latest/connectors) for a step-by-step guide how to set them up.

Run ```make update-ingestion-container``` to restart the local Open Metadata stack with a rebuild of the ingestion container.
