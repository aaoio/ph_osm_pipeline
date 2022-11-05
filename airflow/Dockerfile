FROM apache/airflow:latest-python3.8

ENV AIRFLOW_HOME=/opt/airflow

USER root
COPY requirements.txt /tmp/requirements.txt
COPY sources.list /etc/apt/

RUN apt-get update                             \
 && apt-get install -y --no-install-recommends \
    ca-certificates curl firefox-esr openjdk-11-jdk wget\
 && rm -fr /var/lib/apt/lists/*                \
 && curl -L https://github.com/mozilla/geckodriver/releases/download/v0.32.0/geckodriver-v0.32.0-linux64.tar.gz | tar xz -C /usr/local/bin \
 && apt-get remove -y curl \
 && mkdir -m 772 /home/airflow/.cache \
 && chown -R airflow: ${AIRFLOW_HOME}

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

ENV SPARK_HOME /usr/local/spark
RUN cd "/tmp" \
 && wget "https://dlcdn.apache.org/spark/spark-3.2.2/spark-3.2.2-bin-hadoop3.2.tgz" \
 && tar -xvzf "spark-3.2.2-bin-hadoop3.2.tgz" \
 && mkdir -p "${SPARK_HOME}/bin" \
 && mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" \
 && cp -a "spark-3.2.2-bin-hadoop3.2/bin/." "${SPARK_HOME}/bin/" \
 && cp -a "spark-3.2.2-bin-hadoop3.2/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars" \
 && rm "spark-3.2.2-bin-hadoop3.2.tgz"

RUN export SPARK_HOME
ENV PATH $PATH:/usr/local/spark/bin

USER airflow
WORKDIR $AIRFLOW_HOME
RUN python -m pip install --upgrade pip        \
 && pip install --no-cache-dir -r /tmp/requirements.txt