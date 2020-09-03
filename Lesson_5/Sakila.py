import psycopg2

conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=sakila port=5431")

conn.close()
