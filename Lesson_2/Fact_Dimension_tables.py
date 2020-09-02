import psycopg2

###----------------------------------------------------------------------------------------
### Connect to local Postgres database and get cursor to the database (autocommit is True)
###----------------------------------------------------------------------------------------

try:

    conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=example")

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
### Create the fact table and insert the data
###----------------------------------------------------------------------------------------

try:

    cur.execute('CREATE TABLE IF NOT EXISTS customer_transactions (customer_id int, store_id int, amount_spent float);')

except psycopg2.Error as e:

    print("Error: Issue creating table")
    print (e)

try:

    cur.execute('INSERT INTO customer_transactions (customer_id, store_id, amount_spent) \
                    VALUES (%s, %s, %s)', \
                    (1, 1, 20.50))

except psycopg2.Error as e:

    print("Error: Issue inserting values")
    print (e)

try:

    cur.execute('INSERT INTO customer_transactions (customer_id, store_id, amount_spent) \
                    VALUES (%s, %s, %s)', \
                    (2, 1, 35.21))

except psycopg2.Error as e:

    print("Error: Issue inserting values")
    print (e)

###----------------------------------------------------------------------------------------
### Create the dimension tables and insert the data
###----------------------------------------------------------------------------------------

try:

    cur.execute('CREATE TABLE IF NOT EXISTS items_purchased (customer_id int, item_number int, item_name varchar);')

except psycopg2.Error as e:

    print("Error: Issue creating table")
    print (e)

try:

    cur.execute('INSERT INTO items_purchased (customer_id, item_number, item_name) \
                    VALUES (%s, %s, %s)', \
                    (1, 1, 'Rubber Soul'))

except psycopg2.Error as e:

    print("Error: Issue inserting values")
    print (e)

try:

    cur.execute('INSERT INTO items_purchased (customer_id, item_number, item_name) \
                    VALUES (%s, %s, %s)', \
                    (2, 3, 'Let It Be'))

except psycopg2.Error as e:

    print("Error: Issue inserting values")
    print (e)



try:

    cur.execute('CREATE TABLE IF NOT EXISTS store (store_id int, state varchar);')

except psycopg2.Error as e:

    print("Error: Issue creating table")
    print (e)

try:

    cur.execute('INSERT INTO store (store_id, state) \
                    VALUES (%s, %s)', \
                    (1, 'CA'))

except psycopg2.Error as e:

    print("Error: Issue inserting values")
    print (e)

try:

    cur.execute('INSERT INTO store (store_id, state) \
                    VALUES (%s, %s)', \
                    (2, 'WA'))

except psycopg2.Error as e:

    print("Error: Issue inserting values")
    print (e)



try:

    cur.execute('CREATE TABLE IF NOT EXISTS customer (customer_id int, name varchar, rewards varchar);')

except psycopg2.Error as e:

    print("Error: Issue creating table")
    print (e)

try:

    cur.execute('INSERT INTO customer (customer_id, name, rewards) \
                    VALUES (%s, %s, %s)', \
                    (1, 'Amanda', 'Y'))

except psycopg2.Error as e:

    print("Error: Issue inserting values")
    print (e)

try:

    cur.execute('INSERT INTO customer (customer_id, name, rewards) \
                    VALUES (%s, %s, %s)', \
                    (2, 'Toby', 'N'))

except psycopg2.Error as e:

    print("Error: Issue inserting values")
    print (e)

###----------------------------------------------------------------------------------------
### Query 1: Find all customers that spent more than 30 dollars, who are they, which store they bought from, location of the store, what they bought and if they are a rewards member
###----------------------------------------------------------------------------------------

try:

    cur.execute("SELECT customer.name, store.store_id, store.state, items_purchased.item_name, customer.rewards \
                                FROM ((customer_transactions JOIN customer ON customer_transactions.customer_id = customer.customer_id) \
                                JOIN items_purchased ON customer.customer_id = items_purchased.customer_id) \
                                JOIN store ON customer_transactions.store_id = store.store_id WHERE customer_transactions.amount_spent > 30.00;")

except psycopg2.Error as e:

    print("Error: Issue in SELECT")
    print (e)

row = cur.fetchone()

while row:

   print(row)
   row = cur.fetchone()

###----------------------------------------------------------------------------------------
### Query 2: How much did customer 2 spent?
###----------------------------------------------------------------------------------------

try:

    cur.execute("SELECT customer_transactions.customer_id, SUM(customer_transactions.amount_spent) \
                                FROM customer_transactions WHERE customer_transactions.customer_id = 2 GROUP BY customer_id;")

except psycopg2.Error as e:

    print("Error: Issue in SELECT")
    print (e)

row = cur.fetchone()

while row:

   print(row)
   row = cur.fetchone()

###----------------------------------------------------------------------------------------
### Drop tables and close cursor and connection
###----------------------------------------------------------------------------------------

try:
    cur.execute("DROP table customer_transactions")
except psycopg2.Error as e:
    print("Error: Dropping table")
    print (e)
try:
    cur.execute("DROP table customer")
except psycopg2.Error as e:
    print("Error: Dropping table")
    print (e)
try:
    cur.execute("DROP table store")
except psycopg2.Error as e:
    print("Error: Dropping table")
    print (e)
try:
    cur.execute("DROP table items_purchased")
except psycopg2.Error as e:
    print("Error: Dropping table")
    print (e)

cur.close()
conn.close()
