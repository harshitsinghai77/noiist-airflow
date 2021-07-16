# import uuid

from airflow import DAG
from airflow.operators.python import PythonOperator

# from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import dag
from airflow.utils.dates import days_ago

# from custom.postgres_to_s3_operator import PostgresToS3Operator
import pandas as pd

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
}

dag = DAG(
    dag_id="spotiybase_recommendation",
    start_date=days_ago(2),
    schedule_interval=None,
)


def _get_users(query, **context):
    """Get all users from the database."""
    postgres_hook = PostgresHook(postgres_conn_id="spotify_base_recommendation")
    results = postgres_hook.get_records(query)
    context["task_instance"].xcom_push(key="users", value=results)


def _get_recent_songs(ds, **context):
    """For each user get all the recent songs from the Spotify."""
    from spotify.etl import run_spotify_etl

    users_lst = []
    users = context["task_instance"].xcom_pull(task_ids="get_users", key="users")
    for user in users:
        user_email = user[0]
        username = user[1]
        refresh_token = user[4]
        file_name = run_spotify_etl(
            username=username,
            refresh_token=refresh_token,
            ds=ds,
        )
        if file_name:
            users_lst.append(
                {
                    "user_email": user_email,
                    "file_name": file_name,
                    "refresh_token": refresh_token,
                    "username": username,
                }
            )
    context["task_instance"].xcom_push(key="recent_songs", value=users_lst)


def _recommend_songs(feature_set, **context):
    from spotify.utils import get_minio_client
    from spotify.recommendation import get_recommendations

    minio_client = get_minio_client()

    if not minio_client.bucket_exists(bucket_name="spotify-recommendation"):
        minio_client.make_bucket(bucket_name="spotify-recommendation")

    # Read preprocessed data
    complete_feature_set_object = minio_client.get_object(
        bucket_name="spotify-preprocessed", object_name=feature_set
    )
    all_spotify_object = minio_client.get_object(
        bucket_name="spotify-preprocessed", object_name="spotify_df.csv"
    )
    complete_feature_set = pd.read_csv(complete_feature_set_object)
    all_spotify = pd.read_csv(all_spotify_object)

    users_lst = context["task_instance"].xcom_pull(
        task_ids="get_recent_songs", key="recent_songs"
    )

    for users in users_lst:
        get_recommendations(
            complete_feature_set=complete_feature_set,
            all_spotify=all_spotify,
            file_name=users["file_name"],
            refresh_token=users["refresh_token"],
        )


def _generate_email(**context):
    from spotify.utils import get_minio_client
    from spotify.generate_email import create_email, send_email

    minio_client = get_minio_client()

    users_lst = context["task_instance"].xcom_pull(
        task_ids="get_recent_songs", key="recent_songs"
    )

    for users in users_lst:
        file_name = f'recommendation_{users["file_name"]}'
        username = users["username"]
        user_email = users["user_email"]
        user_recommendation_object = minio_client.get_object(
            bucket_name="spotify-recommendation", object_name=file_name
        )
        user_recommendation = pd.read_csv(user_recommendation_object)
        html_content = create_email(df=user_recommendation, username=username)
        send_email(reciever_email=user_email, html_content=html_content)
        print("Sent email to ", user_email)


get_users = PythonOperator(
    task_id="get_users",
    python_callable=_get_users,
    op_kwargs={
        "query": "SELECT email, display_name, spotify_url, access_token, refresh_token, is_active from core_user;"
    },
    dag=dag,
)

get_recent_songs = PythonOperator(
    task_id="get_recent_songs",
    python_callable=_get_recent_songs,
    dag=dag,
)

recommend_songs = PythonOperator(
    task_id="recommend_songs",
    python_callable=_recommend_songs,
    op_kwargs={
        "feature_set": "complete_feature.csv",
        "bucket_name": "spotify-users",
    },
    dag=dag,
)

generate_email = PythonOperator(
    task_id="generate_email",
    python_callable=_generate_email,
    dag=dag,
)

get_users >> get_recent_songs >> recommend_songs >> generate_email
