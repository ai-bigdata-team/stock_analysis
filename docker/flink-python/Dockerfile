FROM flink:2.1.1-java11

USER root

# -------------------------------------------------
# 1. Python runtime cho PyFlink
# -------------------------------------------------
RUN apt-get update && \
    apt-get install -y \
        python3 \
        python3-pip \
        python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# -------------------------------------------------
# 2. Python dependencies (PyFlink + libs)
# -------------------------------------------------
COPY requirements.txt /opt/flink/requirements.txt
RUN pip install --no-cache-dir -r /opt/flink/requirements.txt

# -------------------------------------------------
# 3. PyFlink job code (KHÔNG mount)
# -------------------------------------------------
# Ví dụ: scripts/streaming/processing/kafka_to_bq_job.py
COPY scripts/ /opt/flink/scripts/

# -------------------------------------------------
# 4. Flink connectors / external jars
# -------------------------------------------------
# Kafka connector, BigQuery connector, etc.
COPY jars/*.jar /opt/flink/lib/
COPY jars/ opt/flink/jars/

# -------------------------------------------------
# 5. Runtime directories (contract)
# -------------------------------------------------
RUN mkdir -p /opt/flink/keys /opt/flink/conf && \
    chown -R flink:flink /opt/flink