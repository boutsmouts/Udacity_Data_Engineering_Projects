'''
Main program to drop and create tables on the Redshift cluster
'''

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):

    '''
    FUNCTION:   drop_tables
    PURPOSE:    Runs SQL queries to drop tables
    INPUT:      cur:    a specified cursor on Redshift cluster
                conn:   a specified connection to Redshift cluster
    OUTPUT:     None
    '''

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):

    '''
    FUNCTION:   create_tables
    PURPOSE:    Runs SQL queries to create tables
    INPUT:      cur:    a specified cursor on Redshift cluster
                conn:   a specified connection to Redshift cluster
    OUTPUT:     None
    '''

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():

    '''
    FUNCTION:   main
    PURPOSE:    Runs the specified functions to drop and create tables on a Redshift cluster
    INPUT:      None
    OUTPUT:     No explicit return, prints to the console
    '''

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to Redshift cluster...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connection successfully established!')

    print('Dropping tables...')
    drop_tables(cur, conn)
    print('Tables successfully dropped!')

    print('Creating tables...')
    create_tables(cur, conn)
    print('Tables successfully created!')

    print('Closing connection...')
    conn.close()
    print('Connection successfully closed!')


if __name__ == "__main__":
    main()
