import psycopg2
import pandas as pd
load_ext sql

DB_ENDPOINT = 'localhost'
DB = 'postgres'
DB_USER = 'postgres'
DB_PASSWORD = 'sakila'
DB_PORT = '5431'

# DB_string = postgresql://username:password@host:port/database
DB_string = 'postgresql://{}:{}@{}:{}/{}' \
                        .format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB)

sql $DB_string

###
# How many entries?
###

n_movies = %sql SELECT COUNT(*) FROM film
n_stores = %sql SELECT COUNT(*) FROM store
n_customers = %sql SELECT COUNT(*) FROM customer
n_rentals = %sql SELECT COUNT(*) FROM rental
n_payments = %sql SELECT COUNT(*) FROM payment
n_staff = %sql SELECT COUNT(*) FROM staff
n_cities = %sql SELECT COUNT(*) FROM city
n_countries = %sql SELECT COUNT(*) FROM country

print('movies: ' + str(n_movies[0][0]))
print('stores: ' + str(n_stores[0][0]))
print('customers: ' + str(n_customers[0][0]))
print('rentals: ' + str(n_rentals[0][0]))
print('payments: ' + str(n_payments[0][0]))
print('staff: ' + str(n_staff[0][0]))
print('cities: ' + str(n_cities[0][0]))
print('countries: ' + str(n_countries[0][0]))

###
# Time period?
###

sql SELECT MIN(payment_date) AS start, MAX(payment_date) AS end FROM payment

###
# Where are customers from?
###
sql SELECT city.city, SUM(address.address_id) AS n FROM address JOIN city ON address.city_id = city.city_id GROUP BY city.city ORDER BY n DESC LIMIT 10

sql SELECT district, SUM(address_id) AS n FROM address GROUP BY district ORDER BY n DESC LIMIT 10

sql SELECT district, SUM(city_id) AS n FROM address GROUP BY district ORDER BY n DESC LIMIT 10

###
# What are the top movies?
###

sql SELECT film_id, title, release_year, rental_rate, rating FROM film limit 5

sql SELECT * FROM payment limit 5

sql SELECT * FROM inventory limit 5

%%sql
SELECT film.title, payment.amount, payment.payment_date, payment.customer_id FROM payment
JOIN rental ON (payment.rental_id = rental.rental_id)
JOIN inventory ON (rental.inventory_id = inventory.inventory_id)
JOIN film ON (inventory.film_id = film.film_id)
LIMIT 5;

%%sql
SELECT film.title, SUM(payment.amount) AS revenue FROM payment
JOIN rental ON (payment.rental_id = rental.rental_id)
JOIN inventory ON (rental.inventory_id = inventory.inventory_id)
JOIN film ON (inventory.film_id = film.film_id)
GROUP BY film.title
ORDER BY revenue DESC
LIMIT 10;

###
# What cities are top grossing?
###

%%sql
SELECT city.city, SUM(payment.amount) AS revenue FROM payment
JOIN customer ON (payment.customer_id = customer.customer_id)
JOIN address ON (customer.address_id = address.address_id)
JOIN city ON (address.city_id = city.city_id)
GROUP BY city.city
ORDER BY revenue DESC
LIMIT 10;

###
# Total revenue by month and movies by city and month (data cube)
###

%%sql
SELECT SUM(payment.amount) AS revenue, EXTRACT(month FROM payment.payment_date) AS month FROM payment
GROUP BY month
ORDER BY revenue DESC
LIMIT 10;

%%sql
SELECT film.title, payment.amount, payment.customer_id, city.city, payment.payment_date, EXTRACT(month FROM payment.payment_date) AS month
FROM payment
JOIN rental ON (payment.rental_id = rental.rental_id)
JOIN inventory ON (rental.inventory_id = inventory.inventory_id)
JOIN film ON (inventory.film_id = film.film_id)
JOIN customer ON (payment.customer_id = customer.customer_id)
JOIN address ON (customer.address_id = address.address_id)
JOIN city ON (address.city_id = city.city_id)
ORDER BY payment.payment_date
LIMIT 10;

%%sql
SELECT film.title, city.city, EXTRACT(month FROM payment.payment_date) AS month, SUM(payment.amount) AS revenue
FROM payment
JOIN rental ON (payment.rental_id = rental.rental_id)
JOIN inventory ON (rental.inventory_id = inventory.inventory_id)
JOIN film ON (inventory.film_id = film.film_id)
JOIN customer ON (payment.customer_id = customer.customer_id)
JOIN address ON (customer.address_id = address.address_id)
JOIN city ON (address.city_id = city.city_id)
GROUP BY (film.title, city.city, month)
ORDER BY month, revenue DESC
LIMIT 10;

###
# Create Fact and Dimension Tables (Star Schema)
###

%%sql
CREATE TABLE IF NOT EXISTS dimDate
(
    date_key int NOT NULL PRIMARY KEY,
    date date NOT NULL,
    year smallint NOT NULL,
    quarter smallint NOT NULL,
    month smallint NOT NULL,
    day smallint NOT NULL,
    week smallint NOT NULL,
    is_weekend boolean
);

%%sql
CREATE TABLE IF NOT EXISTS dimCustomer
(
    customer_key SERIAL PRIMARY KEY,
    customer_id smallint NOT NULL,
    first_name varchar(45) NOT NULL,
    last_name varchar(45) NOT NULL,
    email varchar(50),
    address varchar(50) NOT NULL,
    address2 varchar(50),
    district varchar(20) NOT NULL,
    city varchar(50) NOT NULL,
    country varchar(50) NOT NULL,
    postal_code varchar(10),
    phone varchar(20) NOT NULL,
    active smallint NOT NULL,
    create_date timestamp NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL
);

CREATE TABLE IF NOT EXISTS dimMovie
(
    movie_key SERIAL PRIMARY KEY,
    film_id smallint NOT NULL,
    title varchar(255) NOT NULL,
    description text,
    release_year year,
    language varchar(20) NOT NULL,
    original_language varchar(20),
    rental_duration smallint NOT NULL,
    length smallint NOT NULL,
    rating varchar(5) NOT NULL,
    special_features varchar(60) NOT NULL
);

CREATE TABLE IF NOT EXISTS dimStore
(
    store_key SERIAL PRIMARY KEY,
    store_id smallint NOT NULL,
    address varchar(50) NOT NULL,
    address2 varchar(50),
    district varchar(20) NOT NULL,
    city varchar(50) NOT NULL,
    country varchar(50) NOT NULL,
    postal_code varchar(10),
    manager_first_name varchar(45) NOT NULL,
    manager_last_name varchar(45) NOT NULL,
    start_date date NOT NULL,
    end_date date NOT NULL
);

%%sql
CREATE TABLE IF NOT EXISTS factSales
(
    sales_key SERIAL PRIMARY KEY,
    date_key int REFERENCES dimDate (date_key),
    customer_key int REFERENCES dimCustomer (customer_key),
    movie_key int REFERENCES dimMovie (movie_key),
    store_key int REFERENCES dimStore (store_key),
    sales_amount float
);

%%sql
SELECT * FROM factSales;

###
# Populate the Tables
###

%%sql
INSERT INTO dimDate (date_key, date, year, quarter, month, day, week, is_weekend)
SELECT DISTINCT(TO_CHAR(payment_date :: DATE, 'yyyyMMDD')::integer) AS date_key,
       date(payment_date)                                           AS date,
       EXTRACT(year FROM payment_date)                              AS year,
       EXTRACT(quarter FROM payment_date)                           AS quarter,
       EXTRACT(month FROM payment_date)                             AS month,
       EXTRACT(day FROM payment_date)                               AS day,
       EXTRACT(week FROM payment_date)                              AS week,
       CASE WHEN EXTRACT(ISODOW FROM payment_date) IN (6, 7) THEN true ELSE false END AS is_weekend
FROM payment;

%%sql
INSERT INTO dimCustomer (customer_key, customer_id, first_name, last_name, email, address,
                         address2, district, city, country, postal_code, phone, active,
                         create_date, start_date, end_date)
SELECT  customer.customer_id AS customer_key,
        customer.customer_id,
        customer.first_name,
        customer.last_name,
        customer.email,
        address.address,
        address.address2,
        address.district,
        city.city,
        country.country,
        address.postal_code,
        address.phone,
        customer.active,
        customer.create_date,
        now() AS start_date,
        now() AS end_date
FROM customer
JOIN address  ON (customer.address_id = address.address_id)
JOIN city   ON (address.city_id = city.city_id)
JOIN country ON (city.country_id = country.country_id)
ON CONFLICT (customer_key) DO NOTHING;

%%sql
INSERT INTO dimMovie (movie_key, film_id, title, description, release_year, language, original_language, rental_duration,
                        length, rating, special_features)
SELECT  film.film_id AS movie_key,
        film.film_id,
        film.title,
        film.description,
        film.release_year,
        language.name AS language,
        original_language.name AS original_language,
        film.rental_duration,
        film.length,
        film.rating,
        film.special_features
FROM film
JOIN language  ON (film.language_id = language.language_id)
LEFT JOIN language original_language  ON (film.original_language_id = original_language.language_id)
ON CONFLICT (movie_key) DO NOTHING;

%%sql
INSERT INTO dimStore (store_key, store_id, address, address2, district, city, country, postal_code,
                        manager_first_name, manager_last_name, start_date, end_date)
SELECT  store.store_id AS store_key,
        store.store_id,
        address.address,
        address.address2,
        address.district,
        city.city,
        country.country,
        address.postal_code,
        staff.first_name AS manager_first_name,
        staff.last_name AS manager_last_name,
        now() AS start_date,
        now() AS end_date
FROM store
JOIN staff ON (store.manager_staff_id = staff.staff_id)
JOIN address ON (store.address_id = address.address_id)
JOIN city ON (address.city_id = city.city_id)
JOIN country ON (city.country_id = country.country_id)
ON CONFLICT (store_key) DO NOTHING;

%%sql
INSERT INTO factSales (date_key, customer_key, movie_key, store_key, sales_amount)
SELECT  TO_CHAR(payment.payment_date :: DATE, 'yyyyMMDD')::integer AS date_key,
        payment.customer_id AS customer_key,
        inventory.film_id AS movie_key,
        inventory.store_id AS store_key,
        payment.amount
FROM payment
JOIN rental ON (payment.rental_id = rental.rental_id)
JOIN inventory ON (rental.inventory_id = inventory.inventory_id)
ON CONFLICT (sales_key) DO NOTHING;

###
# Create OLAP CUBE
###

%%sql
SELECT dimDate.day, dimMovie.rating, dimCustomer.city, SUM(sales_amount) AS revenue FROM factSales
JOIN dimDate ON (factSales.date_key = dimDate.date_key)
JOIN dimMovie ON (factSales.movie_key = dimMovie.movie_key)
JOIN dimCustomer ON (factSales.customer_key = dimCustomer.customer_key)
GROUP BY (dimDate.day, dimMovie.rating, dimCustomer.city)
ORDER BY revenue DESC
LIMIT 20;

###
# Slice the Cube
###

%%sql
SELECT dimDate.day, dimMovie.rating, dimCustomer.city, SUM(sales_amount) AS revenue FROM factSales
JOIN dimDate ON (factSales.date_key = dimDate.date_key)
JOIN dimMovie ON (factSales.movie_key = dimMovie.movie_key)
JOIN dimCustomer ON (factSales.customer_key = dimCustomer.customer_key)
WHERE dimMovie.rating = 'PG-13'
GROUP BY (dimDate.day, dimMovie.rating, dimCustomer.city)
ORDER BY revenue DESC
LIMIT 20;

###
# Dice the Cube
###

%%sql
SELECT dimDate.day, dimMovie.rating, dimCustomer.city, SUM(sales_amount) AS revenue FROM factSales
JOIN dimDate ON (factSales.date_key = dimDate.date_key)
JOIN dimMovie ON (factSales.movie_key = dimMovie.movie_key)
JOIN dimCustomer ON (factSales.customer_key = dimCustomer.customer_key)
WHERE dimMovie.rating IN ('PG-13', 'PG')
AND dimCustomer.city IN ('Mannheim', 'Zanzibar')
AND dimDate.day IN ('1', '15', '30')
GROUP BY (dimDate.day, dimMovie.rating, dimCustomer.city)
ORDER BY revenue DESC
LIMIT 20;

###
# Roll-Up the Cube from City to Country
###

%%sql
SELECT dimDate.day, dimMovie.rating, dimCustomer.country, SUM(sales_amount) AS revenue FROM factSales
JOIN dimDate ON (factSales.date_key = dimDate.date_key)
JOIN dimMovie ON (factSales.movie_key = dimMovie.movie_key)
JOIN dimCustomer ON (factSales.customer_key = dimCustomer.customer_key)
GROUP BY (dimDate.day, dimMovie.rating, dimCustomer.country)
ORDER BY revenue DESC
LIMIT 20;

###
# Drill-Down the Cube from City to District
###

%%sql
SELECT dimDate.day, dimMovie.rating, dimCustomer.district, SUM(sales_amount) AS revenue FROM factSales
JOIN dimDate ON (factSales.date_key = dimDate.date_key)
JOIN dimMovie ON (factSales.movie_key = dimMovie.movie_key)
JOIN dimCustomer ON (factSales.customer_key = dimCustomer.customer_key)
GROUP BY (dimDate.day, dimMovie.rating, dimCustomer.district)
ORDER BY revenue DESC
LIMIT 20;

###
# Grouping by different dimensions
###

# TOTAL

%%sql
SELECT SUM(sales_amount) AS revenue FROM factSales

# BY COUNTRY

%%sql
SELECT dimStore.country, SUM(sales_amount) AS revenue FROM factSales
JOIN dimStore ON (factSales.store_key = dimStore.store_key)
GROUP BY (dimStore.country)
ORDER BY dimStore.country, revenue DESC
LIMIT 20;

# BY MONTH

%%sql
SELECT dimDate.month, SUM(sales_amount) AS revenue FROM factSales
JOIN dimDate ON (factSales.date_key = dimDate.date_key)
GROUP BY (dimDate.month)
ORDER BY dimDate.month, revenue DESC
LIMIT 20;

# BY MONTH AND COUNTRY

%%sql
SELECT dimDate.month, dimStore.country, SUM(sales_amount) AS revenue FROM factSales
JOIN dimDate ON (factSales.date_key = dimDate.date_key)
JOIN dimStore ON (factSales.store_key = dimStore.store_key)
GROUP BY (dimDate.month, dimStore.country)
ORDER BY dimDate.month, dimStore.country, revenue DESC
LIMIT 20;

# BY TOTAL, BY MONTH, BY COUNTRY, BY MONTH & COUNTRY ALL-IN-ONE

%%sql
SELECT dimDate.month, dimStore.country, SUM(sales_amount) AS revenue FROM factSales
JOIN dimDate ON (factSales.date_key = dimDate.date_key)
JOIN dimStore ON (factSales.store_key = dimStore.store_key)
GROUP BY grouping sets ((), dimDate.month, dimStore.country, (dimDate.month, dimStore.country))
LIMIT 20;

#BY CUBE

%%sql
SELECT dimDate.month, dimStore.country, SUM(sales_amount) AS revenue FROM factSales
JOIN dimDate ON (factSales.date_key = dimDate.date_key)
JOIN dimStore ON (factSales.store_key = dimStore.store_key)
GROUP BY CUBE(dimDate.month, dimStore.country)
LIMIT 20;
