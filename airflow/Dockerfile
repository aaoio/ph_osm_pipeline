FROM apache/airflow:2.4.1

ENV AIRFLOW_HOME=/opt/airflow

USER root
COPY requirements.txt /tmp/requirements.txt
COPY sources.list /etc/apt/

RUN apt-get update                             \
 && apt-get install -y --no-install-recommends \
    ca-certificates curl firefox-esr           \
 && rm -fr /var/lib/apt/lists/*                \
 && curl -L https://github.com/mozilla/geckodriver/releases/download/v0.32.0/geckodriver-v0.32.0-linux64.tar.gz | tar xz -C /usr/local/bin \
 && apt-get remove -y ca-certificates curl

USER airflow
WORKDIR $AIRFLOW_HOME
RUN python -m pip install --upgrade pip        \
 && pip install --no-cache-dir -r /tmp/requirements.txt