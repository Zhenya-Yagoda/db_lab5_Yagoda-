import csv
import psycopg2

username = 'postgres'
password = '1234567890'
database = 'movie'
host = 'localhost'
port = '5432'

output_file = 'C:\\Users\\User\\Desktop\\lab5Final\\Yagoda_movie_{}.csv'

tables = [
    'director',
    'star',
    'writer',
    'movie',
    'score',
]

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

with conn:
    cur = conn.cursor()

    for table_name in tables:
        cur.execute('SELECT * FROM ' + table_name)
        fieldnames = [x[0] for x in cur.description]
        with open(output_file.format(table_name), 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(fieldnames)
            for row in cur:
                writer.writerow(row)