# openmetadata-ingestion-contrib

Framework for customizing existing connectors of the OpenMetadata platform.
Simply it allows replacing or extending the original implementation while still using the OpenMetadata UI integration of the connectors.

Existing connectors will be shadowed by the newly developed solutions.

## Installation

For developing and installation of custom connectors refer to the [official Open Metadata documentation](https://docs.open-metadata.org/latest/connectors/custom-connectors).

## Project structure
- `./src` contains custom modules that can be referenced by `service_spec.py` files. These are typically custom implementations
of Open Metadata connectors that also inherit from their original implementations.

 
- `./patch` contains a Python script that will traverse the `ingestion` folder and searches for files named `service_spec.py`.
It then will search for the according service_spec files in the original `metadata/ingestion` directory. If the relative paths
starting from the `metadata` directory match, the original file is replaced by the according file in this.
The patch script is executed after installing the `ingestion_contrib` package in the Docker container.


- `local-openmetadata-stack` contains a Docker Compose file to deploy a Open Metadata instance locally. It should be 
started using the `Makefile` in the root directory.

## Configuration

The patch process can be configured with the [config.yml](./patch/config.yml) file in the `patch` directory.
Mandatory config keys are `rootdir` and `patch-service-specs`.

`rootdir` has to point to the `ingestion` directory in which the `service_spec.py` files (and subdirectories) reside.

`patch-service-specs` contains a list of modules whose `service_spec.py` files should be patched. The modules names
are derived from the directory in which the `service_spec.py` files are placed (for 
`ingestion/source/database/mariadb/service_spec.py` the config entry is `mariadb`).

## Local Dev Stack

1. Setup a virtual environment for Python and install dependencies with 
    ```shell
    pip install .
    ```
2. Start local Docker containers with 
   ```shell
   make local-openmetadata-stack
   ```
3. Set up a new service and ingestion in the Open Metadata dashboard at http://localhost:8585 to test custom connectors. 
Refer to the [documentation for the various connectors](https://docs.open-metadata.org/latest/connectors) for a step-by-step guide how to set them up.

Run ```make update-ingestion-container``` to restart the local Open Metadata stack with a rebuild of the ingestion container.
