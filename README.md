# Aluno 1 — Produtor (Kafka + Faker/Python)

## Objetivo
Garantir que o fluxo de dados exista e seja estável: criar o tópico `vendas_raw`
no Kafka e enviar continuamente transações sintéticas de e-commerce — incluindo
algumas de **alto valor** (> 500) para acionar os alertas de fraude (Aluno 2).

## Arquivos desta parte
```
aluno1-producer/
├── Dockerfile          # imagem Python 3.11 + dependências
├── requirements.txt    # confluent-kafka + Faker
└── producer.py         # gerador e enviador de transações
```

## Como funciona
1. `producer.py` espera o Kafka ficar disponível (`AdminClient.list_topics`).
2. Cria o tópico `vendas_raw` (3 partições) caso ainda não exista.
3. Em loop, gera uma transação a cada `INTERVALO_SEGUNDOS` (padrão: 1s) e a publica.
   - ~15% das mensagens têm `valor_total` entre 500,01 e 8000,00 (alto valor).
   - A `key` da mensagem é o `id_cliente`.

### Exemplo de mensagem produzida
```json
{
  "id_transacao": "62d9830f-179f-4894-87dc-4244984c1594",
  "id_cliente": 987,
  "valor_total": 450.50,
  "categoria": "Eletrônicos",
  "timestamp": "2026-06-15T18:00:00Z"
}
```

## Variáveis de ambiente
| Variável             | Padrão        | Descrição                                   |
|----------------------|---------------|---------------------------------------------|
| `KAFKA_BOOTSTRAP`    | `kafka:9092`  | Endereço do broker Kafka                     |
| `TOPIC`              | `vendas_raw`  | Tópico de destino                            |
| `INTERVALO_SEGUNDOS` | `1`           | Intervalo entre envios (em segundos)         |

## Trecho do `docker-compose.yml` referente a esta parte
> O `docker-compose.yml` fica na **raiz do projeto** e é commitado pelo
> integrador. Este trecho é só para você entender como o seu serviço é ligado.

```yaml
  kafka:
    image: confluentinc/cp-kafka:7.6.1
    hostname: kafka
    container_name: kafka
    ports:
      - "29092:29092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@kafka:29093'
      KAFKA_LISTENERS: 'PLAINTEXT://kafka:9092,CONTROLLER://kafka:29093,PLAINTEXT_HOST://0.0.0.0:29092'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
      CLUSTER_ID: 'MkU3OEVBNTcwNTJENDM2Qk'
    healthcheck:
      test: ["CMD-SHELL", "kafka-topics --bootstrap-server kafka:9092 --list || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 15

  kafka-ui:
    image: ghcr.io/kafbat/kafka-ui:latest
    container_name: kafka-ui
    depends_on:
      - kafka
    ports:
      - "8090:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
      DYNAMIC_CONFIG_ENABLED: 'true'

  producer:
    build: ./aluno1-producer
    image: ecommerce-producer:latest
    container_name: producer
    depends_on:
      kafka:
        condition: service_healthy
    environment:
      KAFKA_BOOTSTRAP: kafka:9092
      TOPIC: vendas_raw
      INTERVALO_SEGUNDOS: "1"
    restart: on-failure
```

## Como subir só esta parte (para testar isolado)
```bash
# a partir da raiz do projeto
docker compose up --build kafka kafka-ui producer
```

## Validação (evidência para entregar)
1. Abra o **KafBat UI**: <http://localhost:8090>
2. Vá em **Topics → `vendas_raw`**.
3. Aba **Messages**: veja as mensagens chegando em tempo real.
4. Tire prints mostrando o volume crescendo e o conteúdo JSON das mensagens.

## Logs úteis
```bash
docker compose logs -f producer
```
