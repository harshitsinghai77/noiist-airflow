import uuid

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago

dag = DAG(
    dag_id="data_pipeline_with_airflow",
    start_date=days_ago(10),
    schedule_interval=None,
)


def _team_captain_america(**context):
    team_id = str(uuid.uuid4())
    context["task_instance"].xcom_push(key="team_id", value=team_id)

    return "Captain America"


def _team_iron_man():
    return "Iron Man"


def _pick_team(team):
    if team == "captain_america":
        return "captain_america"
    else:
        return "iron_man"


def _deploy_model(**context):
    team_id = context["task_instance"].xcom_pull(
        task_ids="captain_america", key="team_id"
    )
    print(f"Deploying team {team_id}")


start = DummyOperator(task_id="start", dag=dag)
fetch_weather_data = DummyOperator(task_id="fetch_weather_data", dag=dag)
clean_weather_data = DummyOperator(task_id="clean_weather_data", dag=dag)
fetch_sales_data = DummyOperator(task_id="fetch_sales_data", dag=dag)
clean_sales_data = DummyOperator(task_id="clean_sales_data", dag=dag)

pick_team = BranchPythonOperator(
    task_id="pick_team",
    python_callable=_pick_team,
    op_kwargs={"team": "captain_america"},
)
captain_america = PythonOperator(
    task_id="captain_america", python_callable=_team_captain_america, dag=dag
)
clean_america_team = DummyOperator(task_id="clean_america_team", dag=dag)
iron_man = PythonOperator(task_id="iron_man", python_callable=_team_iron_man, dag=dag)
clean_iron_man = DummyOperator(task_id="clean_iron_man", dag=dag)
join_data_sets = DummyOperator(
    task_id="join_data_sets", trigger_rule="none_failed", dag=dag
)
train_model = DummyOperator(task_id="train_model", dag=dag)
deploy_model = PythonOperator(
    task_id="deploy_model", python_callable=_deploy_model, dag=dag
)

start >> [fetch_weather_data, fetch_sales_data]
fetch_weather_data >> clean_weather_data
fetch_sales_data >> clean_sales_data

[clean_weather_data, clean_sales_data] >> pick_team

pick_team >> [captain_america, iron_man]
captain_america >> clean_america_team
iron_man >> clean_iron_man

[clean_america_team, clean_iron_man] >> join_data_sets >> train_model >> deploy_model
