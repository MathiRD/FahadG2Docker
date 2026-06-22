# Aluno 2 — Stream & Cache (Flink + Valkey)

## Objetivo
Capturar transações críticas em tempo real: um job no Apache Flink consome o
tópico `vendas_raw`, filtra apenas as transações de **alto valor** (`valor > 500`)
e as grava imediatamente no **Valkey** (Redis) como chave-valor
(`fraude:<id_transacao>`).

## Arquivos desta parte
```
aluno2-flink/
├── Dockerfile          # Flink 1.18 + Python/PyFlink + conector Kafka + redis
├── requirements.txt    # apache-flink + redis
└── fraude_job.py       # job PyFlink (DataStream API)
```

## Como funciona
1. A imagem (`Dockerfile`) parte de `flink:1.18.1`, instala Python/PyFlink,
   o cliente `redis` e baixa o **conector Kafka** para `/opt/flink/lib`
   (assim o `KafkaSource` funciona sem `add_jars` no código).
2. O `fraude_job.py`:
   - lê todas as mensagens de `vendas_raw` (offset `earliest`);
   - filtra `valor_total > 500`;
   - grava cada alerta no Valkey como um **hash** na chave `fraude:<id_transacao>`
     (com TTL de 24h);
   - também imprime os alertas no log (`.print()`), visível no TaskManager.
3. O serviço `flink-job-submitter` (definido no compose) espera Kafka + JobManager
   subirem e então submete o job com:
   ```
   flink run -d -m jobmanager:8081 -py /opt/flink/jobs/fraude_job.py
   ```

## Variáveis de ambiente (com padrões já embutidos no job)
| Variável            | Padrão                       | Descrição                          |
|---------------------|------------------------------|------------------------------------|
| `KAFKA_BOOTSTRAP`   | `kafka:9092`                 | Broker Kafka                       |
| `TOPIC`             | `vendas_raw`                 | Tópico de origem                   |
| `GROUP_ID`          | `flink-fraude-alto-valor`    | Consumer group                     |
| `REDIS_HOST`        | `valkey`                     | Host do Valkey/Redis               |
| `REDIS_PORT`        | `6379`                       | Porta do Valkey/Redis              |
| `LIMITE_ALTO_VALOR` | `500`                        | Limite para considerar "alto valor"|
| `TTL_SEGUNDOS`      | `86400`                      | Expiração das chaves no Redis      |

## Trecho do `docker-compose.yml` referente a esta parte
> O `docker-compose.yml` fica na **raiz do projeto** e é commitado pelo
> integrador. Os três serviços abaixo (jobmanager, taskmanager e o submitter)
> usam a **mesma imagem** construída a partir de `./aluno2-flink`.

```yaml
  jobmanager:
    build: ./aluno2-flink
    image: ecommerce-flink:1.18
    container_name: jobmanager
    hostname: jobmanager
    depends_on:
      - kafka
    ports:
      - "8082:8081"     # Flink Dashboard -> http://localhost:8082
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        parallelism.default: 1

  taskmanager:
    build: ./aluno2-flink
    image: ecommerce-flink:1.18
    container_name: taskmanager
    depends_on:
      - jobmanager
    command: taskmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 2
        parallelism.default: 1

  flink-job-submitter:
    build: ./aluno2-flink
    image: ecommerce-flink:1.18
    container_name: flink-job-submitter
    depends_on:
      - jobmanager
      - taskmanager
      - kafka
    entrypoint: /bin/bash
    command:
      - -c
      - |
        echo "Aguardando o Kafka (porta 9092)..."
        until (echo > /dev/tcp/kafka/9092) 2>/dev/null; do sleep 3; done
        echo "Aguardando o Flink JobManager (porta 8081)..."
        until (echo > /dev/tcp/jobmanager/8081) 2>/dev/null; do sleep 3; done
        echo "Tudo pronto. Submetendo o job de fraude ao Flink..."
        sleep 5
        /opt/flink/bin/flink run -d -m jobmanager:8081 -py /opt/flink/jobs/fraude_job.py \
          && echo "OK: job submetido." \
          || (echo "Falha ao submeter. Vai reiniciar e tentar de novo." && exit 1)
    restart: on-failure

  valkey:
    image: valkey/valkey:8
    container_name: valkey
    ports:
      - "6379:6379"

  redisinsight:
    image: redis/redisinsight:latest
    container_name: redisinsight
    depends_on:
      - valkey
    ports:
      - "5540:5540"     # Redis Insight -> http://localhost:5540
```

## Como subir só esta parte (depende do Aluno 1 produzindo dados)
```bash
# a partir da raiz do projeto
docker compose up --build kafka producer jobmanager taskmanager flink-job-submitter valkey redisinsight
```

## Validação (evidência para entregar)
1. **Flink Dashboard**: <http://localhost:8082> → o job `fraude-alto-valor`
   deve aparecer com status **RUNNING**. Tire o print.
2. **Redis Insight**: <http://localhost:5540>
   - Na primeira vez, adicione o banco: **Add Redis database** →
     Host `valkey`, Port `6379` (sem senha).
   - Veja as chaves `fraude:*` aparecendo e o conteúdo (hash) de cada alerta.
   - Tire prints mostrando os alertas armazenados.

## Observações
- O job lê a partir do `earliest`. Em uma reinicialização sem checkpoints ele
  reprocessa as mensagens, mas como a chave no Redis é `fraude:<id_transacao>`,
  a gravação é **idempotente** (não duplica).

## Logs úteis
```bash
docker compose logs -f flink-job-submitter   # confirmação do submit
docker compose logs -f taskmanager           # alertas impressos pelo job
```
