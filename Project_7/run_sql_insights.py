import psycopg2
import pandas as pd
import pandas.io.sql as sqlio
import configparser

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
#config.read_file(open('dwh.cfg'))

conn = psycopg2.connect(database = config.get('CLUSTER', 'DB_NAME'),
                        user = config.get('CLUSTER', 'DB_USER'),
                        password = config.get('CLUSTER', 'DB_PASSWORD'),
                        host = config.get('CLUSTER', 'HOST'),
                        port = config.get('CLUSTER', 'DB_PORT'))

def run_sql_query(sql_query, sql_conn, sql_text = '', sql_head = 5):

    '''
    FUNCTION:   run_sql_query
    PURPOSE:    Runs a given SQL query and prints the given number of results to the console
    INPUT:      sql_query:          a specified SQL query
                sql_conn:           a connector to a PostgreSQL database using psycopg2
                sql_text:           an optional output text for the console
                sql_head:           an optional argument to specify the amount of results to print
    OUTPUT:     No explicit return. Function prints to console.
    '''

    df = sqlio.read_sql_query(sql_query, sql_conn)
    print(sql_text)
    print(df.dropna().head(sql_head))
    print('')


'''
SQL query to get the amount of car crashes per month
'''

sql_query_month = '''SELECT time.month, COUNT(*) AS counts_month FROM incidents \
                JOIN time ON (incidents.timestamp = time.timestamp) \
                GROUP BY time.month \
                ORDER BY counts_month DESC
            '''
text_month = 'The top five months with the most number of car crashes are:'
run_sql_query(sql_query_month, conn, text_month)

'''
SQL query to get the amount of car crashes per weekday
'''

sql_query_weekday = '''SELECT time.weekday, COUNT(*) AS counts_weekday FROM incidents \
                JOIN time ON (incidents.timestamp = time.timestamp) \
                GROUP BY time.weekday \
                ORDER BY counts_weekday DESC
            '''
text_weekday = 'The number of car crashes per weekday are (0 = Sunday):'
run_sql_query(sql_query_weekday, conn, text_weekday, 7)

'''
SQL query to get the amount of car crashes per hour of the day
'''

sql_query_hour = '''SELECT time.hour, COUNT(*) AS counts_hour FROM incidents \
                JOIN time ON (incidents.timestamp = time.timestamp) \
                GROUP BY time.hour \
                ORDER BY counts_hour DESC
            '''
text_hour = 'The top five hours of the day with the most number of car crashes are:'
run_sql_query(sql_query_hour, conn, text_hour)

'''
SQL query to get the amount of car crashes per street in New York City
'''

sql_query_street = '''SELECT locations.street, COUNT(*) AS counts_street FROM incidents \
                JOIN locations ON (incidents.location_id = locations.location_id) \
                GROUP BY locations.street \
                ORDER BY counts_street DESC
            '''
text_street = 'The top ten streets with the most car crashes are:'
run_sql_query(sql_query_street, conn, text_street, 10)

'''
SQL query to get the amount of car crashes occurring between -5°C and +15°C ambient temperature
'''

sql_query_5_15_temp = '''SELECT COUNT(*) AS counts_between_5_15_temp FROM incidents \
                            JOIN weather ON (incidents.timestamp = weather.timestamp) \
                            WHERE weather.temperature > 268.15 AND weather.temperature < 288.15
                    '''
text_5_15_temp = 'The number of car crashes that occur between -5°C and +15°C are:'
run_sql_query(sql_query_5_15_temp, conn, text_5_15_temp, 1)

'''
SQL query to get the amount of car crashes occurring below -5°C ambient temperature
'''

sql_query_below_5_temp = '''SELECT COUNT(*) AS counts_below_5_temp FROM incidents \
                            JOIN weather ON (incidents.timestamp = weather.timestamp) \
                            WHERE weather.temperature < 268.15
                        '''
text_below_5_temp = 'The number of car crashes that occur below -5°C are:'
run_sql_query(sql_query_below_5_temp, conn, text_below_5_temp, 1)

'''
SQL query to get the amount of car crashes occurring above +15°C ambient temperature
'''

sql_query_above_15_temp = '''SELECT COUNT(*) AS counts_above_15_temp FROM incidents \
                                JOIN weather ON (incidents.timestamp = weather.timestamp) \
                                WHERE weather.temperature > 288.15
                        '''
text_above_15_temp = 'The number of car crashes that occur above +15°C are:'
run_sql_query(sql_query_above_15_temp, conn, text_above_15_temp, 1)

'''
SQL query to get the five years with the most injuries
'''

sql_query_casualties = '''SELECT time.year, SUM(injuries) AS counts_injuries, SUM(fatalities) AS counts_fatalities FROM incidents \
                            JOIN time ON (incidents.timestamp = time.timestamp) \
                            GROUP BY time.year \
                            ORDER BY counts_injuries DESC
                    '''
text_casualties = 'The five years with the most injuries reported are:'
run_sql_query(sql_query_casualties, conn, text_casualties, 5)
