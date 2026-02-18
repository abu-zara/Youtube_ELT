from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyscope2.extras import RealDicCursur

def get_conn_cursur():
    hook = PostgresHook(posgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursur(cursfor_factory=RealDicCursur)

def close_conn_cursor(conn,cur):
    cur.close()
    conn.close()