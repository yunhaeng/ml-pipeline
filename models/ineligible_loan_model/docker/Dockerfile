FROM python:3.10.9-slim
LABEL maintainer="dkfk591@gmail.com"

ARG USER_HOME=/home/mlops
ARG PREPARATION_SOURCE_PATH=data_preparation
ARG MODEL_OUTPUT_PATH=model_output
ARG REQUIREMENTS_PATH=docker

RUN groupadd --gid 1000 mlops \
    && useradd --uid 1000 --gid mlops --shell /bin/bash --create-home mlops

#data_preparation 폴더의 preparation.py 파일을 복사
COPY --chown=mlops:mlops ${PREPARATION_SOURCE_PATH}/preparation.py \
    ${USER_HOME}/${PREPARATION_SOURCE_PATH}/

COPY --chown=mlops:mlops ${MODEL_OUTPUT_PATH}/ \
    ${USER_HOME}/${MODEL_OUTPUT_PATH}/

COPY --chown=mlops:mlops ${REQUIREMENTS_PATH}/requirements.txt \
    ${USER_HOME}/

USER mlops

RUN mkdir -p ${USER_HOME}/mlops_data_store \
    && pip install --no-cache-dir \
    -r ${USER_HOME}/requirements.txt

WORKDIR ${USER_HOME}