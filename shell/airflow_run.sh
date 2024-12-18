#!/bin/bash

## Run airflow services.
base_name=`basename "${0}"`

function getAirflowProcessIDs {
    PROCESS_IDS=$(ps -ef | grep "${PROCESS_NAME}" | grep -v 'grep' | grep -v "${base_name}" | awk '{print $2}')
}

function runAirflowCommand() {
    # Check running status
    while [ $success = false ] && [ $attempt_num -le $max_attempts ]; do

        getAirflowProcessIDs
        if [ -z "$PROCESS_IDS" ]; then #process_ids의 길이가 0이면(==process가 실행되지 않았으면) true
            # Run the airflow
            # Run airflow 명령어가 작동하지 않음.
            if [ $attempt_num = 1 ]; then #첫 시작
                eval "${AIRFLOW_COMMAND}"
                echo "Run the ${PROCESS_NAME}."
            fi
                echo "The ${PROCESS_NAME} not found.(${attempt_num}/${max_attempts}) Trying again..."
                sleep 2
        else
            echo "The ${PROCESS_NAME} is running."
            success=true
        fi

        #Increment the attempt counter
        attempt_num=$(( attempt_num + 1 ))
    done
}

function startAirflowWebServer() {
    # Set a process name
    PROCESS_NAME="airflow webserver"

    # Set a success
    success=false

    # Set a counter for the number of attempts
    attempt_num=1

    # Set the maximum number of attempts
    max_attempts=10

    # Set a airflow command
    AIRFLOW_COMMAND="airflow webserver --port 8080 -D"

    runAirflowCommand

    if [ $success = false ]; then
        echo "The ${PROCESS_NAME} could not be started. The ${PROCESS_NAME} is forced to start."
        nohup airflow webserver --port 8080 >/dev/null 2>&1 &
    fi
}

function startAirflowScheduler() {
    # Set a process name
    PROCESS_NAME="airflow scheduler"

    # Set a success
    success=false

    # Set a counter for the number of attempts
    attempt_num=1

    # Set a counter for the number of attempts
    max_attempts=5

    # Set a airflow command
    AIRFLOW_COMMAND="airflow scheduler -D"

    runAirflowCommand

    if [ $success = false ]; then
        echo "The ${PROCESS_NAME} could not be started. The ${PROCESS_NAME} is forced to start."
        nohup airflow scheduler >/dev/null 2>&1 &
    fi
}

function startAirflowTriggerer() {
    # Set a process name
    PROCESS_NAME="airflow triggerer"

    # Set a success
    success=false

    # Set a counter for the number of attempts
    attempt_num=1

    # Set a counter for the number of attempts
    max_attempts=5

    # Set a airflow command
    AIRFLOW_COMMAND="airflow triggerer -D"

    runAirflowCommand

    if [ $success = false ]; then
        echo "The ${PROCESS_NAME} could not be started. The ${PROCESS_NAME} is forced to start."
        nohup airflow triggerer >/dev/null 2>&1 &
    fi
}

startAirflowWebServer
startAirflowScheduler
startAirflowTriggerer