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
### Create four tables containing data in thrid normalized form
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

    cur.execute("CREATE TABLE IF NOT EXISTS sales (transaction_id int, amount_spent int);")

except psycopg2.Error as e:

    print("Error: Issue creating table")
    print (e)

try:

    cur.execute("CREATE TABLE IF NOT EXISTS albums_sold (album_id int, transaction_id int, album_name varchar);")

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
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (1, 1, "Rubber Soul"))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (2, 1, "Let It Be"))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (3, 2, "My Generation"))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (4, 3, "Meet the Beatles"))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (5, 3, "Help!"))
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

try:
    cur.execute("INSERT INTO sales (transaction_id, amount_spent) \
                 VALUES (%s, %s)", \
                 (1, 40))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO sales (transaction_id, amount_spent) \
                 VALUES (%s, %s)", \
                 (2, 19))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO sales (transaction_id, amount_spent) \
                 VALUES (%s, %s)", \
                 (3, 45))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

###----------------------------------------------------------------------------------------
### Check that data are inserted correctly
###----------------------------------------------------------------------------------------

print("Table: Transactions2\n")
try:
    cur.execute("SELECT * FROM transactions2;")
except psycopg2.Error as e:
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

print("\nTable: Albums_Sold\n")
try:
    cur.execute("SELECT * FROM albums_sold;")
except psycopg2.Error as e:
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

print("\nTable: Employees\n")
try:
    cur.execute("SELECT * FROM employees;")
except psycopg2.Error as e:
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

print("\nTable: Sales\n")
try:
    cur.execute("SELECT * FROM sales;")
except psycopg2.Error as e:
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

###----------------------------------------------------------------------------------------
### Perform a three-way JOIN to retrieve all data
###----------------------------------------------------------------------------------------

try:
    cur.execute("SELECT transactions2.transaction_id, customer_name, employees.employee_name, year, albums_sold.album_name, sales.amount_spent \
                                FROM ((transactions2 JOIN sales ON transactions2.transaction_id = sales.transaction_id) \
                                JOIN employees ON transactions2.cashier_id = employees.employee_id) \
                                JOIN albums_sold ON albums_sold.transaction_id = transactions2.transaction_id;")

except psycopg2.Error as e:
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

###----------------------------------------------------------------------------------------
### Create denormalized data for better performance on queries by reduced amount of JOINS
###----------------------------------------------------------------------------------------

try:
    cur.execute("CREATE TABLE IF NOT EXISTS transactions (transaction_id int, \
                                                           customer_name varchar, cashier_id int, \
                                                           year int, amount_spent int);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print (e)

try:
    cur.execute("INSERT INTO transactions (transaction_id, customer_name, cashier_id, year, amount_spent) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (1, "Amanda", 1, 2000, 40))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO transactions (transaction_id, customer_name, cashier_id, year, amount_spent) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (2, "Toby", 1, 2000, 19))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO transactions (transaction_id, customer_name, cashier_id, year, amount_spent) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (3, "Max", 2, 2018, 45))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

###----------------------------------------------------------------------------------------
### Query 1: Select transaction_id, customer_name, amount_spent
###----------------------------------------------------------------------------------------

try:
    cur.execute('SELECT transaction_id, customer_name, amount_spent FROM transactions')

except psycopg2.Error as e:
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

###----------------------------------------------------------------------------------------
### Query 2: Select cashier_name, SUM(amount_spent) grouped by cashier_name
###----------------------------------------------------------------------------------------

try:
    cur.execute("CREATE TABLE IF NOT EXISTS cashier_sales (transaction_id int, cashier_name varchar, \
                                                           cashier_id int, amount_spent int);")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print (e)

try:
    cur.execute("INSERT INTO cashier_sales (transaction_id, cashier_name, cashier_id, amount_spent) \
                 VALUES (%s, %s, %s, %s)", \
                 (1, "Sam", 1, 40 ))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO cashier_sales (transaction_id, cashier_name, cashier_id, amount_spent) \
                 VALUES (%s, %s, %s, %s)", \
                 (2, "Sam", 1, 19 ))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute("INSERT INTO cashier_sales (transaction_id, cashier_name, cashier_id, amount_spent) \
                 VALUES (%s, %s, %s, %s)", \
                 (3, "Max", 2, 45))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)

try:
    cur.execute('select cashier_name, SUM(amount_spent) FROM cashier_sales GROUP BY cashier_name;')

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
try:
    cur.execute("DROP table sales")
except psycopg2.Error as e:
    print("Error: Dropping table")
    print (e)
try:
    cur.execute("DROP table cashier_sales")
except psycopg2.Error as e:
    print("Error: Dropping table")
    print (e)

cur.close()
conn.close()
