ARG AIRFLOW_BASE_IMAGE="apache/airflow:master-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

# pyarrow required for writing to_parquet() with Pandas
# minio required for communicating with MinIO
# geopandas & pygeos for mapping lat lon coordinates to NYC taxi zone ids
RUN pip install --user --no-cache-dir \
    minio==7.0.3 \
    scikit-learn==0.24.2 \
    Jinja2==3.0.1
