import json
import psycopg2

username = 'postgres'
password = '1234567890'
database = 'movie'
host = 'localhost'
port = '5432'

output_file = 'C:\\Users\\User\\Desktop\\lab5Final\\Yagoda_movie_{}.csv'
json_output_file = 'C:\\Users\\User\\Desktop\\lab5Final\\all_data.json'

tables = [
    'director',
    'star',
    'writer',
    'movie',
    'score',
]

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

data = {}
with conn:
    cur = conn.cursor()
    for table in tables:
        cur.execute('SELECT * FROM ' + table)
        rows = []
        fields = [x[0] for x in cur.description]
        for row in cur:
            rows.append(dict(zip(fields, row)))
        data[table] = rows

with open(json_output_file, 'w') as outf:
    json.dump(data, outf, default=str)
