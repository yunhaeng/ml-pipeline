#shell 명령어를 통한 유저 생성 예제
airflow users create \
    --username mlops \
    --firstname mlops \
    --lastname study \
    --role Admin \
    --email mlops.study@gmail.com \
    --password study