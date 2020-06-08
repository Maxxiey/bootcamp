import os
import time
import psycopg2
import numpy as np
import sys
from typing import Union
from src.config import DATABASE_VOLUME_DIR, CONTAINER_DATA_DIR

def connect_postgres_server(PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DATABASE):
    try: 
        conn = psycopg2.connect(host=PG_HOST, port=PG_PORT, user=PG_USER, password=PG_PASSWORD, database=PG_DATABASE)
        return conn
    except:
        print ("unable to connect to the database")
        #sys.exit(2)


def create_pg_table(conn, cur, PG_TABLE_NAME):
    try:       
        sql = "CREATE TABLE " + PG_TABLE_NAME + " (ids bigint, answer text);"
        cur.execute(sql)
        conn.commit()
        print("create postgres table!")
    except Exception as e:
        print("can't create postgres table")
        print(''.join(e.args))
        conn.rollback()


def copy_data_to_pg(conn, cur, PG_TABLE_NAME):
    fname = os.path.join(CONTAINER_DATA_DIR,'temp.csv')
    sql = "copy " + PG_TABLE_NAME + " from '" + fname + "' with CSV delimiter '|';"
    print(sql)
    try:
        cur.execute(sql)
        conn.commit()
        print("insert pg sucessful!")
    except Exception as e:
        print("faild  copy!")
        print(''.join(e.args))
        conn.rollback()

def build_pg_index(conn, cur, PG_TABLE_NAME):
    try:
        sql = "CREATE INDEX " + PG_TABLE_NAME + "_index_ids on " + PG_TABLE_NAME + "(ids);"
        cur.execute(sql)
        conn.commit()
        print("build index sucessful!")
    except Exception as e:
        print("faild build index")
        print(''.join(e.args))
        conn.rollback()


def search_in_pg(conn, cur, result, PG_TABLE_NAME):
    # id_ = result[0].id
    sql = "select * from " + PG_TABLE_NAME + " where ids in (" + str(result) + ");"
    print(sql)
    try:
        cur.execute(sql)
        rows=cur.fetchall()
        #print(rows)
        return rows
    except:
        print("search faild!")


def drop_pg_table(conn, cur, PG_TABLE_NAME):
    sql = "drop table " + PG_TABLE_NAME + ";"
    try:
        cur.execute(sql)
        conn.commit()
        print("drop postgres table!")
    except Exception as e:
        print("can't drop postgres table")
        print(''.join(e.args))
        conn.rollback()


def record_txt(ids,answer:Union[str, list]):
    fname = 'temp.csv'
    fname = os.path.join(DATABASE_VOLUME_DIR, fname)
    with open(fname,'w') as f:
        if isinstance(answer, str):
            print(f"read answer file and write into {fname}")
            with open(answer, 'r') as f_answer:
                for i in range(len(ids)):
                    line = f_answer.readline()
                    line = str(ids[i]) + "|" + line
                    f.write(line)
        
        elif isinstance(answer,list):
            print(f"iter answer list and write into {fname}")
            for i,a in zip(ids, answer):
                line = str(i) + "|" + a + "\n"
                print(line)
                f.write(line)

        else:
            pass