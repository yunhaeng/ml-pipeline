import pendulum
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from support.callback_functions import success_callback, failure_callback
from airflow.operators.bash import BashOperator

local_timezone = pendulum.timezone("Asia/Seoul")
conn_id = "feature_store"
model_name = "ineligible_loan_model"
model_version = "1.0.0"
airflow_dags_path = Variable.get("AIRFLOW_DAGS_PATH")
sql_file_path = (f"{airflow_dags_path}/models/ineligible_loan_model"
                 f"/data_extract/ featuers.sql")

def read_sql_file(file_path):
    with open(file_path, "r") as file:
        sql_query_lines = file.read()
    return "".join(sql_query_lines)

with DAG(dag_id="ineligible_loan_model",
         default_args={
             "owner":"mlops",
             "dependents_on_past": False,
             "email": "dkfk591@gmail.com",
             "on_failure_callback": failure_callback,
             "on_success_callback": success_callback,
         },
         description="부적격대출모델",
         schedule=None,
         start_date=datetime(2023, 10, 1, tzinfo=local_timezone),
         catchup=False,
         tags=["mlops", "study"],
         ) as dag:
    
    #task_id에는 space가 들어가면 안된다.
    data_extract = SQLExecuteQueryOperator(
        task_id="데이터추출",
        conn_id=conn_id,
        sql=read_sql_file(sql_file_path),
        split_statements=True
    )

    data_preperation = BashOperator(
        task_id="데이터전처리",
        bash_command=f"cd {airflow_dags_path}/models/ineligible_loan_model/docker && "
                        "docker-compose up --build && docker-compose down",
        env={"PYTHON_FILE": "/home/mlops/data_preparation/preparation.py",
             "MODEL_NAME": model_name,
             "MODEL_VERSION": model_version,
             "BASE_DAY": "{{ yesterday_ds_nodash }}"},
        append_env=True,
        retries=1
    )

    prediction = EmptyOperator(
        task_id="모델예측"
    )

    data_extract >> data_preperation >> prediction