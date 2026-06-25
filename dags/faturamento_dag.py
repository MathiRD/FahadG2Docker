"""
Aluno 4 - Orquestração (Airflow + Postgres)
===========================================
DAG que simula uma carga Batch (roda a cada 2 minutos para fins de teste):
  1. Lê os dados históricos do MongoDB (coleção `transacoes`, criada pelo Aluno 3).
  2. Calcula o faturamento total por categoria (soma de `valor_total`) e a
     quantidade de transações por categoria.
  3. Insere/atualiza (upsert) esses agregados na tabela relacional
     `faturamento_por_categoria` no Postgres (banco `ecommerce`).
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ----------------------------------------------------------------------
# Configuração (lida do ambiente, definido no docker-compose)
# ----------------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:example@mongo:27017/")
MONGO_DB = os.getenv("MONGO_DB", "ecommerce")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "transacoes")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "airflow")
PG_PASSWORD = os.getenv("PG_PASSWORD", "airflow")
PG_ANALYTICS_DB = os.getenv("PG_ANALYTICS_DB", "ecommerce")


# ----------------------------------------------------------------------
# Task: agrega no Mongo e grava no Postgres
# ----------------------------------------------------------------------
def calcular_faturamento_por_categoria(**_):
    from pymongo import MongoClient
    import psycopg2
    from psycopg2.extras import execute_values

    # 1) Agregação no MongoDB: soma e contagem por categoria.
    mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    colecao = mongo[MONGO_DB][MONGO_COLLECTION]

    pipeline = [
        {
            "$group": {
                "_id": "$categoria",
                "total_faturado": {"$sum": "$valor_total"},
                "qtd_transacoes": {"$sum": 1},
            }
        },
        {"$sort": {"total_faturado": -1}},
    ]
    resultados = list(colecao.aggregate(pipeline))
    mongo.close()

    print(f"Categorias agregadas: {len(resultados)}")
    for r in resultados:
        print(f"  {r['_id']}: R$ {r['total_faturado']:.2f} "
              f"({r['qtd_transacoes']} transações)")

    if not resultados:
        print("Nenhum dado no MongoDB ainda. Encerrando a task sem erro.")
        return

    # 2) Upsert no Postgres.
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_ANALYTICS_DB,
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Garante a tabela (idempotente — também é criada pelo script de init do Postgres).
    cur.execute("""
        CREATE TABLE IF NOT EXISTS faturamento_por_categoria (
            categoria        VARCHAR(100) PRIMARY KEY,
            total_faturado   NUMERIC(14, 2) NOT NULL,
            qtd_transacoes   INTEGER NOT NULL,
            atualizado_em    TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)

    linhas = [
        (
            r["_id"] if r["_id"] is not None else "Desconhecida",
            round(float(r["total_faturado"]), 2),
            int(r["qtd_transacoes"]),
        )
        for r in resultados
    ]

    execute_values(
        cur,
        """
        INSERT INTO faturamento_por_categoria
            (categoria, total_faturado, qtd_transacoes)
        VALUES %s
        ON CONFLICT (categoria) DO UPDATE SET
            total_faturado = EXCLUDED.total_faturado,
            qtd_transacoes = EXCLUDED.qtd_transacoes,
            atualizado_em  = NOW();
        """,
        linhas,
    )

    cur.close()
    conn.close()
    print(f"{len(linhas)} categorias gravadas/atualizadas no Postgres.")


# ----------------------------------------------------------------------
# Definição da DAG
# ----------------------------------------------------------------------
default_args = {
    "owner": "aluno4",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="faturamento_por_categoria",
    description="Carga batch: faturamento por categoria (MongoDB -> Postgres)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="*/2 * * * *",   # a cada 2 minutos
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "batch", "analytics"],
) as dag:

    tarefa_faturamento = PythonOperator(
        task_id="calcular_faturamento_por_categoria",
        python_callable=calcular_faturamento_por_categoria,
    )
