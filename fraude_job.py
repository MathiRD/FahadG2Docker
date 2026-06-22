"""
Aluno 2 - Stream & Cache (Flink + Valkey/Redis)
===============================================
Job PyFlink (DataStream API) que:
  1. Consome todas as transações do tópico `vendas_raw` no Kafka.
  2. Filtra apenas as de ALTO VALOR (valor_total > 500).
  3. Grava cada alerta no Valkey (Redis) na chave `fraude:<id_transacao>`.

O conector Kafka já está em /opt/flink/lib (ver Dockerfile), portanto não é
necessário usar env.add_jars aqui.
"""

import json
import os

from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.datastream.functions import MapFunction

# ----------------------------------------------------------------------
# Configuração
# ----------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "vendas_raw")
GROUP_ID = os.getenv("GROUP_ID", "flink-fraude-alto-valor")

REDIS_HOST = os.getenv("REDIS_HOST", "valkey")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

LIMITE_ALTO_VALOR = float(os.getenv("LIMITE_ALTO_VALOR", "500"))
TTL_SEGUNDOS = int(os.getenv("TTL_SEGUNDOS", "86400"))  # 24h


# ----------------------------------------------------------------------
# Sink que escreve os alertas no Redis/Valkey.
# A conexão é criada de forma preguiçosa (lazy) dentro do worker, porque
# o objeto precisa ser serializável para ser distribuído pelo Flink.
# ----------------------------------------------------------------------
class RedisFraudeSink(MapFunction):
    def __init__(self, host, port, ttl):
        self.host = host
        self.port = port
        self.ttl = ttl
        self._client = None

    def open(self, runtime_context):
        import redis
        self._client = redis.Redis(
            host=self.host,
            port=self.port,
            decode_responses=True,
        )

    def map(self, value):
        try:
            t = json.loads(value)
        except (ValueError, TypeError):
            return value  # mensagem malformada: ignora

        id_transacao = t.get("id_transacao", "desconhecido")
        chave = f"fraude:{id_transacao}"

        self._client.hset(chave, mapping={
            "id_transacao": str(id_transacao),
            "id_cliente": str(t.get("id_cliente", "")),
            "valor_total": str(t.get("valor_total", "")),
            "categoria": str(t.get("categoria", "")),
            "timestamp": str(t.get("timestamp", "")),
        })
        self._client.expire(chave, self.ttl)

        return f"ALERTA salvo em {chave} (R$ {t.get('valor_total')})"


# ----------------------------------------------------------------------
# Filtro de alto valor
# ----------------------------------------------------------------------
def eh_alto_valor(value: str) -> bool:
    try:
        t = json.loads(value)
        return float(t.get("valor_total", 0)) > LIMITE_ALTO_VALOR
    except (ValueError, TypeError):
        return False


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(TOPIC)
        .set_group_id(GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "kafka-vendas-raw",
    )

    (
        stream
        .filter(eh_alto_valor)
        .map(RedisFraudeSink(REDIS_HOST, REDIS_PORT, TTL_SEGUNDOS))
        .print()
    )

    env.execute("fraude-alto-valor")


if __name__ == "__main__":
    main()
