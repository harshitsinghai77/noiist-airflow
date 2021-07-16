from minio import Minio
from airflow.hooks.base import BaseHook


def get_minio_client():

    s3_conn = BaseHook.get_connection(conn_id="s3")
    minio_client = Minio(
        s3_conn.extra_dejson["host"].split("://")[1],
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
    )
    return minio_client
