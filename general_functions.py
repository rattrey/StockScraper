import psycopg2
import pandas as pd
from datetime import date
from datetime import datetime
from sqlalchemy import create_engine
#create table in postgres-
engine = create_engine('postgresql+psycopg2://attrey:tusr3f@127.0.0.1/postgres')



def posgres_create_table(table_query):
    try:
        connection = psycopg2.connect(user = "attrey", password = "tusr3f", host = "127.0.0.1", port = "5432", database = "postgres")
        cursor = connection.cursor()
        create_table_query = f'''{table_query}'''
        print(create_table_query)
        cursor.execute(create_table_query)
        connection.commit()
        print("table successfully created")
    
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error while creating table", error)


#read query from postgres
def postgres_query_df(query):
    conn = psycopg2.connect(user = "attrey", password = "tusr3f", host = "127.0.0.1", port = "5432", database = "postgres")
    cur = conn.cursor()
    postgres_script = f"{query}"
    cur.execute(postgres_script)
    return pd.DataFrame(cur.fetchall())

#read query from postgres into list
def postgres_query_list(query):
    conn = psycopg2.connect(user = "attrey", password = "tusr3f", host = "127.0.0.1", port = "5432", database = "postgres")
    cur = conn.cursor()
    postgres_script = f"{query}"
    cur.execute(postgres_script)
    return cur.fetchall()
    
#get column names from table
def get_columns(table_name, column_names = []):
    conn = psycopg2.connect(user = "attrey", password = "tusr3f", host = "127.0.0.1", port = "5432", database = "postgres")
    cur = conn.cursor()
    cur.execute(f'''Select * FROM public."{table_name}" LIMIT 0;''')
    colnames = [desc[0] for desc in cur.description]
    return colnames

def find_nth(haystack, needle, n):
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start + len(needle))
        n -= 1
    return start

