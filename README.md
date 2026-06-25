# Sistema de Alertas e Analytics de E-commerce — Pipeline Final (G2)

Pipeline de Big Data que monitora transações de e-commerce em tempo real para:
- **detectar compras de alto valor** (time de fraude) — via Flink + Valkey/Redis;
- **persistir o histórico bruto** — via MongoDB (data lake NoSQL);
- **consolidar relatórios agregados** (time de negócios) — via Airflow + Postgres.

Tudo orquestrado com **Docker Compose**.

> **Disciplina:** Big Data — Prof. Fahad Kalil — ATITUS Educação

---

## Equipe

| Aluno | Nome                       | RA        | Responsabilidade                       |
|-------|----------------------------|-----------|----------------------------------------|
| 1     | Matheus Durigon Rodrigues  | 1134695   | Produtor (Kafka + Faker)               |
| 2     | Erick De Nardi             | 1134724   | Stream & Cache (Flink + Valkey)        |
| 3     | Luis Zanin                 | 1136493   | Data Lake NoSQL (MongoDB)              |
| 4     | João Inácio                | 1135445   | Orquestração (Airflow + Postgres)      |

---

## Arquitetura

```
                           ┌─────────────────────────────────────────┐
                           │                 KAFKA                     │
   Aluno 1                 │              tópico: vendas_raw           │
 ┌──────────┐   produz     │                                           │
 │ producer │ ───────────► │  ◄── KafBat UI (visualização)             │
 │ (Python  │              └───────┬───────────────────────┬──────────┘
 │  +Faker) │                      │                        │
 └──────────┘            consome   │                        │  consome
                          (todas)  │                        │  (todas)
                                   ▼                        ▼
                        Aluno 2                       Aluno 3
                  ┌──────────────────┐         ┌──────────────────┐
                  │  Flink job        │         │  mongo-consumer   │
                  │  filtra > 500     │         │  (Python)         │
                  └────────┬─────────┘         └────────┬─────────┘
                           │ grava alerta               │ insere documento
                           ▼                            ▼
                  ┌──────────────────┐         ┌──────────────────┐
                  │  Valkey (Redis)   │         │  MongoDB          │
                  │  fraude:<id>      │         │  ecommerce.       │
                  │  ◄ Redis Insight  │         │   transacoes      │
                  └──────────────────┘         │  ◄ Mongo Express  │
                                               └────────┬─────────┘
                                                        │ lê histórico
                                          Aluno 4       ▼
                                     ┌─────────────────────────────┐
                                     │  Airflow DAG (a cada 2 min)  │
                                     │  faturamento por categoria   │
                                     └──────────────┬──────────────┘
                                                    │ upsert agregados
                                                    ▼
                                          ┌──────────────────┐
                                          │  Postgres         │
                                          │  ecommerce.       │
                                          │  faturamento_     │
                                          │   por_categoria   │
                                          │  ◄ PgAdmin        │
                                          └──────────────────┘
```

---

## Pré-requisitos
- **Docker** e **Docker Compose** (v2, comando `docker compose`).
- **Memória**: recomenda-se **pelo menos 8 GB** disponíveis para o Docker
  (são ~16 containers, incluindo Flink e Airflow). Em Docker Desktop, ajuste
  em *Settings → Resources*.
- Conexão à internet na primeira subida (download das imagens e do conector
  Kafka do Flink).

---

## Como executar (tudo de uma vez)

Na raiz do projeto:

```bash
docker compose up --build
```

A primeira execução demora alguns minutos (build das imagens). A ordem de
inicialização é controlada por `depends_on`/healthchecks:
- o **producer** só inicia quando o Kafka está saudável;
- o **flink-job-submitter** espera Kafka + JobManager e então submete o job;
- o **Airflow** só inicia depois que o `airflow-init` migra o banco e cria o usuário.

Para parar:
```bash
docker compose down
```

Para parar **e apagar os volumes** (recomeçar do zero — Mongo, Postgres, etc.):
```bash
docker compose down -v
```

---

## Acessos (portas e credenciais)

| Serviço            | URL                              | Login / Observação                                  |
|--------------------|----------------------------------|-----------------------------------------------------|
| **KafBat UI**      | <http://localhost:8090>          | sem login                                            |
| **Flink Dashboard**| <http://localhost:8082>          | sem login                                            |
| **Redis Insight**  | <http://localhost:5540>          | adicione o DB: host `valkey`, porta `6379`           |
| **Mongo Express**  | <http://localhost:8081>          | sem login (auth desativada)                          |
| **Airflow UI**     | <http://localhost:8085>          | `admin` / `admin`                                    |
| **PgAdmin**        | <http://localhost:5050>          | `admin@admin.com` / `admin` (senha do PG: `airflow`) |

Portas de dados (caso queira conectar do host):
| Serviço     | Porta host | Credenciais            |
|-------------|------------|------------------------|
| Kafka       | `29092`    | —                      |
| Valkey      | `6379`     | —                      |
| MongoDB     | `27017`    | `root` / `example`     |
| Postgres    | `5432`     | `airflow` / `airflow`  |

---

## Estrutura do repositório

```
ecommerce-pipeline/
├── docker-compose.yml            # orquestra TODOS os serviços
├── .gitignore
├── README.md                     # este arquivo
│
├── aluno1-producer/              # Aluno 1 — Produtor (Kafka + Faker)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── producer.py
│   └── README.md
│
├── aluno2-flink/                 # Aluno 2 — Stream & Cache (Flink + Valkey)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── fraude_job.py
│   └── README.md
│
├── aluno3-mongo/                 # Aluno 3 — Data Lake NoSQL (MongoDB)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── consumer.py
│   └── README.md
│
├── aluno4-airflow/               # Aluno 4 — Orquestração (Airflow + Postgres)
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── dags/
│   │   └── faturamento_dag.py
│   └── README.md
│
├── postgres/
│   └── init/
│       └── 01-create-ecommerce-db.sql   # cria o banco "ecommerce" + tabela
│
├── pgadmin/
│   └── servers.json              # pré-cadastro do servidor no PgAdmin
│
└── evidencias/                   # prints exigidos na entrega
    └── README.md                 # checklist de evidências
```

---

## Divisão por aluno (o que cada um faz / commita)

> O `docker-compose.yml`, a pasta `postgres/`, a pasta `pgadmin/`, este `README.md`
> e o `.gitignore` são a **"cola"** do projeto e ficam na raiz. Cada `alunoN-*/README.md`
> traz o trecho do compose relevante para aquela parte, para o aluno entender como o
> serviço dele é ligado.

| Aluno | Nome                      | RA      | Pasta/arquivos que commita                          |
|-------|---------------------------|---------|-----------------------------------------------------|
| 1     | Matheus Durigon Rodrigues | 1134695 | `aluno1-producer/`                                  |
| 2     | Erick De Nardi            | 1134724 | `aluno2-flink/`                                     |
| 3     | Luis Zanin                | 1136493 | `aluno3-mongo/`                                     |
| 4     | João Inácio               | 1135445 | `aluno4-airflow/`                                   |
| —     | Integração (compartilhado)| —       | `docker-compose.yml`, `postgres/`, `pgadmin/`, `README.md`, `.gitignore` |

---

## Roteiro de validação (para gerar as evidências)

1. **Aluno 1 — KafBat UI** (<http://localhost:8090>): tópico `vendas_raw`, aba
   *Messages*, mensagens chegando em tempo real.
2. **Aluno 2 — Flink Dashboard** (<http://localhost:8082>): job `fraude-alto-valor`
   com status **RUNNING**. **Redis Insight** (<http://localhost:5540>): chaves
   `fraude:*` com os alertas.
3. **Aluno 3 — Mongo Express** (<http://localhost:8081>): banco `ecommerce`,
   coleção `transacoes`, volume de documentos crescendo.
4. **Aluno 4 — Airflow UI** (<http://localhost:8085>, `admin`/`admin`): DAG
   `faturamento_por_categoria` verde + logs. **PgAdmin** (<http://localhost:5050>):
   ```sql
   SELECT * FROM faturamento_por_categoria ORDER BY total_faturado DESC;
   ```

Deixe o pipeline rodando alguns minutos antes de capturar — a DAG roda a cada 2
minutos. Salve os prints em `evidencias/` (ver checklist lá).

---

## Solução de problemas (troubleshooting)

- **Erros estranhos no Kafka/Flink/Mongo após mexer no projeto**: provavelmente
  um volume antigo. Recomece limpo:
  ```bash
  docker compose down -v
  docker compose up --build
  ```
- **Build do Flink falha com `ReadTimeoutError` (pip)**: conexão lenta baixando
  o pacote grande do PyFlink. O `aluno2-flink/Dockerfile` já usa
  `--timeout=1000 --retries=10` no `pip install` para tolerar isso. Se ainda
  assim falhar, use uma rede mais estável e reconstrua **só** a imagem do Flink:
  ```bash
  docker compose build jobmanager
  ```
- **Erro `401 Unauthorized` / repositório "is not signed" no build**: a rede tem
  um proxy que exige autenticação. Use uma rede sem proxy ou configure as
  credenciais do proxy em *Docker Desktop → Settings → Resources → Proxies*.
- **O job do Flink não aparece no Dashboard**: veja os logs do submitter —
  ele espera Kafka + JobManager e tenta de novo automaticamente:
  ```bash
  docker compose logs -f flink-job-submitter
  ```
- **PgAdmin não conecta**: o servidor já vem cadastrado; ao abrir, informe a
  senha do Postgres (`airflow`). Confirme que o `postgres` está *healthy*:
  ```bash
  docker compose ps
  ```
- **Airflow demora a aparecer**: é normal na primeira vez (migração do banco).
  Acompanhe:
  ```bash
  docker compose logs -f airflow-init
  docker compose logs -f airflow-webserver
  ```
- **Pouca memória**: se containers ficarem reiniciando, aumente a RAM do Docker.