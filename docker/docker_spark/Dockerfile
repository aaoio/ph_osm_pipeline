FROM bitnami/spark:3.2.2

USER root
COPY requirements.txt /tmp/requirements.txt
COPY sources.list /etc/apt/

RUN apt-get update \
 && apt-get install -y curl \
 && rm -fr /var/lib/apt/lists/* \
 && curl https://jdbc.postgresql.org/download/postgresql-42.5.0.jar --output /usr/local/spark/resources/postgresql-42.5.0.jar

RUN python -m pip install --upgrade pip \
 && pip install --no-cache-dir -r /tmp/requirements.txt