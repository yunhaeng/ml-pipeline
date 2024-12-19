import pendulum
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from support.callback_functions import success_callback, failure_callback

local_timezone = pendulum.timezone("Asia/Seoul")

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
    data_extract = EmptyOperator(
        task_id="데이터추출"
    )

    data_preperation = EmptyOperator(
        task_id="데이터전처리"
    )

    prediction = EmptyOperator(
        task_id="모델예측"
    )

    data_extract >> data_preperation >> prediction