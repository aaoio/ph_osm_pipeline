FROM bitnami/spark:3.2.2

USER root
COPY requirements.txt /tmp/requirements.txt

RUN python -m pip install --upgrade pip \
 && pip install --no-cache-dir -r /tmp/requirements.txt