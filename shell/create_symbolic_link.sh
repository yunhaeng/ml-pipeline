#airflow 심볼릭 링크 생성 예제
mkdir -p ~/airflow/dags
ln -snf ~/Study/mlops-study-ml-pipeline/features ~/airflow/dags/features
ln -snf ~/Study/mlops-study-ml-pipeline/models ~/airflow/dags/models
ln -snf ~/Study/mlops-study-ml-pipeline/support ~/airflow/dags/support
ls -al ~/airflow/dags