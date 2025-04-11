ARG OPENMETADATA_INGESTION_IMAGE_VERSION
FROM openmetadata/ingestion:${OPENMETADATA_INGESTION_IMAGE_VERSION}

RUN mkdir ./ingestion_contrib
COPY --chown=airflow ./src ./ingestion_contrib/src
COPY --chown=airflow pyproject.toml ./ingestion_contrib
COPY --chown=airflow config.example.yaml ./ingestion_contrib/config.yaml

RUN cd ingestion_contrib && pip install .

RUN cd ingestion_contrib && patch-service-specs