import psycopg2

###----------------------------------------------------------------------------------------
### Connect to local Postgres database and get cursor to the database (autocommit is True)
###----------------------------------------------------------------------------------------

try:

    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=example port = 7000")

except psycopg2.Error as e:

    print("Error: Could not make connection to the Postgres database")
    print(e)

try:

    cur = conn.cursor()

except psycopg2.Error as e:

    print("Error: Could not get cursor to the Database")
    print(e)

conn.set_session(autocommit=True)

###----------------------------------------------------------------------------------------
### Create first raw table containing all the data
###----------------------------------------------------------------------------------------

try:

    cur.execute("CREATE TABLE IF NOT EXISTS music_store (transaction_id int, customer_name varchar, cashier_name varchar, year int, albums_purchsed text[])")

except psycopg2.Error as e:

    print("Error: Issue creating table")
    print (e)

try:

    cur.execute("INSERT INTO music_store (transaction_id, customer_name, cashier_name, year, albums_purchsed) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (1, 'Amanda', 'Sam', 2000, ['Rubber Soul', 'Let It Be']))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

try:

    cur.execute("INSERT INTO music_store (transaction_id, customer_name, cashier_name, year, albums_purchsed) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (2, 'Toby', 'Sam', 2000, ['My Generation']))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

try:

    cur.execute("INSERT INTO music_store (transaction_id, customer_name, cashier_name, year, albums_purchsed) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (3, 'Max', 'Bob', 2018, ['Meet The Beatles', 'Help!']))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)


try:

    cur.execute("SELECT * FROM music_store;")

except psycopg2.Error as e:

    print("Error: select *")
    print (e)

row = cur.fetchone()

while row:

   print(row)
   row = cur.fetchone()

###----------------------------------------------------------------------------------------
### Transform data into first normal form
###----------------------------------------------------------------------------------------

try:

    cur.execute("CREATE TABLE IF NOT EXISTS music_store2 (transaction_id int, customer_name varchar, cashier_name varchar, year int, albums_purchsed varchar);")

except psycopg2.Error as e:

    print("Error: Issue creating table")
    print (e)

try:

    cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, albums_purchsed) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (1, 'Amanda', 'Sam', 2000, 'Rubber Soul'))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

try:

    cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, albums_purchsed) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (1, 'Amanda', 'Sam', 2000, 'Let It Be'))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

try:

    cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, albums_purchsed) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (2, 'Toby', 'Sam', 2000, 'My Generation'))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

try:

    cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, albums_purchsed) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (3, 'Max', 'Bob', 2018, 'Meet The Beatles'))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

try:

    cur.execute("INSERT INTO music_store2 (transaction_id, customer_name, cashier_name, year, albums_purchsed) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (3, 'Max', 'Bob', 2018, 'Help!'))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

try:

    cur.execute("SELECT * FROM music_store2;")

except psycopg2.Error as e:

    print("Error: select *")
    print (e)

row = cur.fetchone()

while row:

   print(row)
   row = cur.fetchone()

###----------------------------------------------------------------------------------------
### Transform data into second normal form (i.e., two tables transactions and albums sold)
###----------------------------------------------------------------------------------------

try:

    cur.execute("CREATE TABLE IF NOT EXISTS transactions (transaction_id int, customer_name varchar, cashier_name varchar, year int);")

except psycopg2.Error as e:

    print("Error: Issue creating table")
    print (e)

try:

    cur.execute("CREATE TABLE IF NOT EXISTS albums_sold (album_id int, transaction_id int, album_name varchar);")

except psycopg2.Error as e:

    print("Error: Issue creating table")
    print (e)

try:

    cur.execute("INSERT INTO transactions (transaction_id, customer_name, cashier_name, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (1, 'Amanda', 'Sam', 2000))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

try:

    cur.execute("INSERT INTO transactions (transaction_id, customer_name, cashier_name, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (2, 'Toby', 'Sam', 2000))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

try:

    cur.execute("INSERT INTO transactions (transaction_id, customer_name, cashier_name, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (3, 'Max', 'Bob', 2018))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

try:

    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (1, 1, 'Rubber Sould'))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

try:

    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (2, 1, 'Let It Be'))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

try:

    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (3, 2, 'My Generation'))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

try:

    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (4, 3, 'Mee The Beatles'))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

try:

    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (5, 3, 'Help!'))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

print("Table: transactions\n")

try:

    cur.execute("SELECT * FROM transactions;")

except psycopg2.Error as e:

    print("Error: select *")
    print (e)

row = cur.fetchone()

while row:

   print(row)
   row = cur.fetchone()

print("\nTable: albums_sold\n")

try:

    cur.execute("SELECT * FROM albums_sold;")

except psycopg2.Error as e:

    print("Error: select *")
    print (e)

row = cur.fetchone()

while row:

   print(row)
   row = cur.fetchone()

###----------------------------------------------------------------------------------------
### Perform JOIN on this table to retrieve info from the first table
###----------------------------------------------------------------------------------------

try:

    cur.execute("SELECT * FROM transactions JOIN albums_sold ON transactions.transaction_id = albums_sold.transaction_id ;")

except psycopg2.Error as e:

    print("Error: select *")
    print (e)

row = cur.fetchone()

while row:

   print(row)
   row = cur.fetchone()

###----------------------------------------------------------------------------------------
### Transform data into third normal form (i.e., three tables transactions, albums sold, and employees)
###----------------------------------------------------------------------------------------

try:

    cur.execute("CREATE TABLE IF NOT EXISTS transactions2 (transaction_id int, \
                                                           customer_name varchar, cashier_id int, \
                                                           year int);")

except psycopg2.Error as e:

    print("Error: Issue creating table")
    print (e)

try:

    cur.execute("CREATE TABLE IF NOT EXISTS employees (employee_id int, \
                                                       employee_name varchar);")

except psycopg2.Error as e:

    print("Error: Issue creating table")
    print (e)

try:

    cur.execute("INSERT INTO transactions2 (transaction_id, customer_name, cashier_id, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (1, "Amanda", 1, 2000))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

try:

    cur.execute("INSERT INTO transactions2 (transaction_id, customer_name, cashier_id, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (2, "Toby", 1, 2000))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

try:

    cur.execute("INSERT INTO transactions2 (transaction_id, customer_name, cashier_id, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (3, "Max", 2, 2018))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

try:

    cur.execute("INSERT INTO employees (employee_id, employee_name) \
                 VALUES (%s, %s)", \
                 (1, "Sam"))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

try:

    cur.execute("INSERT INTO employees (employee_id, employee_name) \
                 VALUES (%s, %s)", \
                 (2, "Bob"))

except psycopg2.Error as e:

    print("Error: Inserting Rows")
    print (e)

print("Table: transactions2\n")

try:

    cur.execute("SELECT * FROM transactions2;")

except psycopg2.Error as e:

    print("Error: select *")
    print (e)

row = cur.fetchone()

while row:

   print(row)
   row = cur.fetchone()

print("\nTable: albums_sold\n")

try:

    cur.execute("SELECT * FROM albums_sold;")

except psycopg2.Error as e:

    print("Error: select *")
    print (e)

row = cur.fetchone()

while row:

   print(row)
   row = cur.fetchone()

print("\nTable: employees\n")

try:

    cur.execute("SELECT * FROM employees;")

except psycopg2.Error as e:

    print("Error: select *")
    print (e)

row = cur.fetchone()

while row:

   print(row)
   row = cur.fetchone()

###----------------------------------------------------------------------------------------
### Perform JOIN on these three tables to retrieve all the information from the first table
###----------------------------------------------------------------------------------------

try:

    cur.execute("SELECT * FROM (transactions2 JOIN albums_sold ON \
                               transactions2.transaction_id = albums_sold.transaction_id) JOIN \
                               employees ON transactions2.cashier_id=employees.employee_id;")

except psycopg2.Error as e:

    print("Error: select *")
    print (e)


row = cur.fetchone()

while row:

   print(row)
   row = cur.fetchone()

###----------------------------------------------------------------------------------------
### Drop tables and close cursor and connection
###----------------------------------------------------------------------------------------

try:

    cur.execute("DROP table music_store")

except psycopg2.Error as e:

    print("Error: Dropping table")
    print (e)
try:

    cur.execute("DROP table music_store2")

except psycopg2.Error as e:

    print("Error: Dropping table")
    print (e)

try:

    cur.execute("DROP table albums_sold")

except psycopg2.Error as e:

    print("Error: Dropping table")
    print (e)

try:

    cur.execute("DROP table employees")

except psycopg2.Error as e:

    print("Error: Dropping table")
    print (e)

try:

    cur.execute("DROP table transactions")

except psycopg2.Error as e:

    print("Error: Dropping table")
    print (e)

try:

    cur.execute("DROP table transactions2")

except psycopg2.Error as e:

    print("Error: Dropping table")
    print (e)

cur.close()
conn.close()
