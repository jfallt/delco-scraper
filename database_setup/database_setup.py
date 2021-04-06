import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def create_database():
    con = psycopg2.connect("user=postgres password='password' host='localhost'")
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = con.cursor()
    name_Database = "delco_real_estate_sales"
    sqlCreateDatabase = "create database "+name_Database+";"
    cursor.execute(sqlCreateDatabase)

if __name__ == '__main__':
    create_database()
