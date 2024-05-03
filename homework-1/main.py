"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import os
import psycopg2

bd = ['employees_data.csv', 'customers_data.csv', 'orders_data.csv']
bd_name = ['employees', 'customers', 'orders']


def add_data_in_bd(database, database_name):
    conn = psycopg2.connect(host='localhost', database='north', user='postgres', password=2101)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO hw BD VALUES (%s,%s)", (1, 'sid'))
                for item in range(len(bd)):
                    with open(os.path.join('north_data', database[item]), 'r') as csvfile:
                        header = next(csvfile)
                        csvreader = csv.reader(csvfile)
                        for row in csvreader:
                            values = '%s, ' * len(row)
                            cur.execute(f"INSERT INTO {database_name[item]} VALUES ({values[:-2]})", row)
                        conn.commit()
    finally:
        conn.close()


