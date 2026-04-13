from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from datetime import datetime
import requests


# ======================================================
# 🔑 Funções auxiliares
# ======================================================

def conectar_api():
    """
    Conecta à API via conexão Airflow (`api_cosmos_conn`)
    e retorna o token de autenticação Bearer.
    """
    conn = BaseHook.get_connection("api_conn")
    token = conn.password
    return {'Authorization': f'Bearer {token}'}


def conectar_postgres():
    """
    Cria e retorna uma conexão e cursor do PostgreSQL usando o Hook do Airflow.
    """
    hook = PostgresHook(postgres_conn_id="postgres_externo")
    conn = hook.get_conn()
    cursor = conn.cursor()
    return conn, cursor


def fechar_conexao(conn, cursor, commit=True):
    """
    Fecha a conexão com o banco de dados de forma segura e opcionalmente executa o commit.

    Parâmetros:
        conn: objeto de conexão retornado pelo PostgresHook.
        cursor: cursor da conexão.
        commit (bool): se True, faz o commit antes de fechar.
    """
    if commit:
        conn.commit()
    cursor.close()
    conn.close()


# ======================================================
# 1️⃣ Extração para staging
# ======================================================
def extrair_para_staging():
    """
    Extrai dados da API e insere diretamente na tabela de staging.
    """
    headers = conectar_api()
    url = "***"

    response = requests.post(url, headers=headers)
    response.raise_for_status()
    registros = response.json().get("value", [])

    if not registros:
        print("Nenhum dado retornado pela API.")
        return

    conn, cursor = conectar_postgres()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS staging_usuarios_api (
            nome TEXT,
            matricula DECIMAL,
            cpf TEXT
        );
    """)
    cursor.execute("TRUNCATE TABLE staging_usuarios_api;")

    for item in registros:
        cursor.execute("""
            INSERT INTO staging_usuarios_api (nome, matricula, cpf)
            VALUES (%s, %s, %s);
        """, (item.get("nome"), item.get("matricula"), item.get("cpf")))

    fechar_conexao(conn, cursor)

    print(f"{len(registros)} registros inseridos na tabela staging_usuarios_api.")


# ======================================================
# 2️⃣ Carga final
# ======================================================
def carregar_dados_finais():
    """
    Carrega os dados transformados na tabela final 'usuarios_api'.
    """
    conn, cursor = conectar_postgres()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS usuarios_api (
            nome TEXT,
            matricula DECIMAL,
            cpf TEXT 
        );
    """)

    try:
        cursor.execute("TRUNCATE TABLE usuarios_api;")

        cursor.execute("""
            INSERT INTO usuarios_api (nome, matricula, cpf)
            SELECT nome, matricula, cpf
            FROM staging_usuarios_api;

        """)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    finally:
        fechar_conexao(conn, cursor)

    print("Dados carregados na tabela usuarios_api.")


# ======================================================
# 3️⃣ DAG principal
# ======================================================
with DAG(
    dag_id="dag_etl",
    start_date=datetime(2025, 10, 28),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "api", "postgres", "staging"],
    description="ETL escalável com staging (API -> PostgreSQL)"
) as dag:
    
    t1_extrair = PythonOperator(
        task_id="extrair_para_staging",
        python_callable=extrair_para_staging
    )

    t2_carregar = PythonOperator(
        task_id="carregar_dados_finais",
        python_callable=carregar_dados_finais
    )

    # Dependências
    t1_extrair >> t2_carregar
