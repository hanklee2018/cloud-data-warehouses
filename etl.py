"""Import packages."""
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Load staging tables by runing all the copy table queries."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert tabels by running insert table queries."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Load staging tables and insert into created tables."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('-------------loading staging tables-------------')
    load_staging_tables(cur, conn)
    print('-------------insert tables-------------')
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()