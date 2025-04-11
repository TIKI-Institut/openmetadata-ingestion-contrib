ARG OPENMETADATA_INGESTION_IMAGE_VERSION
FROM openmetadata/ingestion:${OPENMETADATA_INGESTION_IMAGE_VERSION}

WORKDIR ingestion
USER root

COPY ./src ./src
COPY ./patch ./patch
COPY pyproject.toml .

# need to be root to change owner of directory
# user airflow needs write permissions to build the package
RUN chown -R airflow ./src

USER airflow
RUN pip install --no-deps .

WORKDIR patch
RUN python patch_service_specs.py