"""
Aluno 3 - Data Lake NoSQL (MongoDB)
===================================
Consumidor que lê ABSOLUTAMENTE TODAS as mensagens do tópico `vendas_raw`
no Kafka e as insere como documentos na coleção `transacoes` do banco
`ecommerce` no MongoDB.

- Usa um consumer group próprio (`mongo-sink`), independente do Flink, de
  modo que ambos recebem todas as mensagens.
- A gravação é idempotente: faz upsert por `id_transacao` (índice único),
  então reprocessar mensagens não gera duplicatas.
"""

import json
import os
import time

from confluent_kafka import Consumer, KafkaError, KafkaException
from pymongo import MongoClient, errors as mongo_errors

# ----------------------------------------------------------------------
# Configuração
# ----------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "vendas_raw")
GROUP_ID = os.getenv("GROUP_ID", "mongo-sink")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:example@mongo:27017/")
MONGO_DB = os.getenv("MONGO_DB", "ecommerce")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "transacoes")


# ----------------------------------------------------------------------
# Espera o MongoDB ficar disponível
# ----------------------------------------------------------------------
def aguardar_mongo() -> MongoClient:
    print(f"Aguardando o MongoDB em {MONGO_URI}...", flush=True)
    while True:
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
            client.admin.command("ping")
            print("MongoDB disponível!", flush=True)
            return client
        except mongo_errors.PyMongoError as exc:
            print(f"  ...ainda indisponível ({exc}). Tentando de novo em 3s.", flush=True)
            time.sleep(3)


# ----------------------------------------------------------------------
# Loop principal
# ----------------------------------------------------------------------
def main():
    client = aguardar_mongo()
    colecao = client[MONGO_DB][MONGO_COLLECTION]
    # Índice único garante idempotência (sem documentos duplicados).
    colecao.create_index("id_transacao", unique=True)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC])

    print(f"Consumindo o tópico '{TOPIC}' e gravando em "
          f"'{MONGO_DB}.{MONGO_COLLECTION}'. (Ctrl+C para parar)", flush=True)

    total = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            try:
                doc = json.loads(msg.value().decode("utf-8"))
            except (ValueError, TypeError) as exc:
                print(f"Mensagem malformada, ignorando: {exc}", flush=True)
                continue

            # Upsert por id_transacao (idempotente).
            colecao.update_one(
                {"id_transacao": doc.get("id_transacao")},
                {"$set": doc},
                upsert=True,
            )
            total += 1

            if total % 10 == 0:
                print(f"  {total} documentos gravados no MongoDB "
                      f"(total na coleção: {colecao.estimated_document_count()})",
                      flush=True)
    except KeyboardInterrupt:
        print("\nEncerrando o consumidor...", flush=True)
    finally:
        consumer.close()
        client.close()
        print(f"Finalizado. Total processado nesta sessão: {total}.", flush=True)


if __name__ == "__main__":
    main()
