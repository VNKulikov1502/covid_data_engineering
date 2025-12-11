ARG AIRFLOW_VERSION=2.8.3

FROM apache/airflow:${AIRFLOW_VERSION}
WORKDIR /airflow

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow

COPY requirements.txt /airflow/requirements.txt
RUN pip install -r /airflow/requirements.txt