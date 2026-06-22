"""
Aluno 1 - Produtor (Kafka + Faker)
==================================
Gera transações sintéticas de e-commerce e envia continuamente para o
tópico `vendas_raw` no Kafka.

- ~15% das transações têm valor ALTO (> 500) para acionar os alertas do Aluno 2.
- O tópico é criado automaticamente caso ainda não exista.

Formato da mensagem (JSON):
{
  "id_transacao": "<uuid>",
  "id_cliente": 987,
  "valor_total": 450.50,
  "categoria": "Eletrônicos",
  "timestamp": "2026-06-15T18:00:00Z"
}
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker

# ----------------------------------------------------------------------
# Configuração (via variáveis de ambiente, com valores padrão)
# ----------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "vendas_raw")
INTERVALO_SEGUNDOS = float(os.getenv("INTERVALO_SEGUNDOS", "1"))

CATEGORIAS = [
    "Eletrônicos",
    "Livros",
    "Moda",
    "Casa e Cozinha",
    "Esportes",
    "Beleza",
    "Brinquedos",
    "Alimentos",
]

fake = Faker("pt_BR")


# ----------------------------------------------------------------------
# Espera o Kafka ficar disponível antes de começar a produzir
# ----------------------------------------------------------------------
def aguardar_kafka():
    print(f"Aguardando o Kafka em {KAFKA_BOOTSTRAP}...", flush=True)
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    while True:
        try:
            admin.list_topics(timeout=5)
            print("Kafka disponível!", flush=True)
            return admin
        except Exception as exc:
            print(f"  ...ainda indisponível ({exc}). Tentando de novo em 3s.", flush=True)
            time.sleep(3)


# ----------------------------------------------------------------------
# Cria o tópico vendas_raw (se ainda não existir)
# ----------------------------------------------------------------------
def criar_topico(admin: AdminClient):
    metadata = admin.list_topics(timeout=5)
    if TOPIC in metadata.topics:
        print(f"Tópico '{TOPIC}' já existe.", flush=True)
        return

    novo = NewTopic(TOPIC, num_partitions=3, replication_factor=1)
    futuros = admin.create_topics([novo])
    for nome, futuro in futuros.items():
        try:
            futuro.result()
            print(f"Tópico '{nome}' criado (3 partições).", flush=True)
        except Exception as exc:
            # Em corrida com auto-create, pode já existir — tudo bem.
            print(f"Aviso ao criar o tópico '{nome}': {exc}", flush=True)


# ----------------------------------------------------------------------
# Gera uma transação sintética
# ----------------------------------------------------------------------
def gerar_transacao() -> dict:
    # ~15% de chance de ser uma compra de ALTO VALOR (> 500)
    if random.random() < 0.15:
        valor = round(random.uniform(500.01, 8000.00), 2)
    else:
        valor = round(random.uniform(10.00, 500.00), 2)

    return {
        "id_transacao": str(uuid.uuid4()),
        "id_cliente": fake.random_int(min=1, max=9999),
        "valor_total": valor,
        "categoria": random.choice(CATEGORIAS),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ----------------------------------------------------------------------
# Callback de entrega (apenas para reportar erros)
# ----------------------------------------------------------------------
def entrega_callback(err, msg):
    if err is not None:
        print(f"ERRO ao entregar mensagem: {err}", flush=True)


# ----------------------------------------------------------------------
# Loop principal
# ----------------------------------------------------------------------
def main():
    admin = aguardar_kafka()
    criar_topico(admin)

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "produtor-vendas",
    })

    print(f"Enviando transações para o tópico '{TOPIC}' a cada {INTERVALO_SEGUNDOS}s. "
          f"(Ctrl+C para parar)", flush=True)

    enviados = 0
    try:
        while True:
            transacao = gerar_transacao()
            payload = json.dumps(transacao, ensure_ascii=False).encode("utf-8")

            producer.produce(
                TOPIC,
                key=str(transacao["id_cliente"]),
                value=payload,
                callback=entrega_callback,
            )
            # Processa callbacks de entrega pendentes sem bloquear.
            producer.poll(0)

            enviados += 1
            flag = "  <== ALTO VALOR" if transacao["valor_total"] > 500 else ""
            print(f"[{enviados:>5}] {transacao['categoria']:<15} "
                  f"R$ {transacao['valor_total']:>9.2f}{flag}", flush=True)

            time.sleep(INTERVALO_SEGUNDOS)
    except KeyboardInterrupt:
        print("\nEncerrando o produtor...", flush=True)
    finally:
        print("Enviando mensagens pendentes (flush)...", flush=True)
        producer.flush(10)
        print("Finalizado.", flush=True)


if __name__ == "__main__":
    main()
