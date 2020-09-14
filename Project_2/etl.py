'''
Main program to perform the ETL on the Redshift cluster
'''

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):

    '''
    FUNCTION:   load_staging_tables
    PURPOSE:    Runs SQL queries to copy staging tables from S3
    INPUT:      cur:    a specified cursor on Redshift cluster
                conn:   a specified connection to Redshift cluster
    OUTPUT:     None
    '''

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

        print('Executing query: {}'.format(query))


def insert_tables(cur, conn):

    '''
    FUNCTION:   insert_tables
    PURPOSE:    Runs SQL queries to insert values into specified tables
    INPUT:      cur:    a specified cursor on Redshift cluster
                conn:   a specified connection to Redshift cluster
    OUTPUT:     None
    '''

    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

        print('Executing query: {}'.format(query))


def main():

    '''
    FUNCTION:   main
    PURPOSE:    Connects to a Redshift cluster and runs the specified functions to perform the ETL
    INPUT:      None
    OUTPUT:     No explicit return, prints to the console
    '''

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('Copying staging tables. Please wait...')
    load_staging_tables(cur, conn)
    print('Staging tables successfully loaded!')

    print('Inserting into tables. Please wait...')
    insert_tables(cur, conn)
    print('Values inserted successfully into tables!')

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
