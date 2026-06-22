FROM flink:1.18.1-scala_2.12-java11

# ----------------------------------------------------------------------
# 1) Python 3 + pip (necessários para o PyFlink)
# ----------------------------------------------------------------------
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends \
      python3 python3-pip python3-dev curl \
 && ln -sf /usr/bin/python3 /usr/bin/python \
 && rm -rf /var/lib/apt/lists/*

# ----------------------------------------------------------------------
# 2) Dependências Python (PyFlink + cliente Redis)
#    A imagem base é Ubuntu 22.04 (pip 22), então não há restrição PEP-668.
# ----------------------------------------------------------------------
COPY requirements.txt /opt/flink/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/flink/requirements.txt

# ----------------------------------------------------------------------
# 3) Conector Kafka do Flink (vai para o classpath em /opt/flink/lib)
#    Assim o KafkaSource é resolvido sem precisar de add_jars no código.
# ----------------------------------------------------------------------
RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# ----------------------------------------------------------------------
# 4) Código do job
# ----------------------------------------------------------------------
RUN mkdir -p /opt/flink/jobs
COPY fraude_job.py /opt/flink/jobs/fraude_job.py
