import csv
import psycopg2

username = 'postgres'
password = '1234567890'
database = 'movie'
host = 'localhost'
port = '5432'

INPUT_CSV_FILE = 'C:\\Users\\User\\Desktop\\lab5Final\\movies.csv'

#
director_drop = '''
DROP TABLE IF EXISTS director
'''
star_drop = '''
DROP TABLE IF EXISTS star
'''
score_drop = '''
DROP TABLE IF EXISTS score
'''
movie_drop = '''
DROP TABLE IF EXISTS Movie
'''
writer_drop = '''
DROP TABLE IF EXISTS writer
'''

director_create = '''
CREATE TABLE Director
(
  id_director INT DEFAULT nextval('director_id_director_seq'::regclass) PRIMARY KEY,
  name_d VARCHAR(50) UNIQUE NOT NULL
);
'''

director_insert = '''
INSERT INTO Director (name_d)
VALUES (%s)
ON CONFLICT (name_d) DO NOTHING
RETURNING id_director;
'''

sequence_create = '''
CREATE SEQUENCE director_id_director_seq
START WITH 100001;
'''

sequence_drop = '''
DROP SEQUENCE IF EXISTS director_id_director_seq;
'''

conn = psycopg2.connect(user=username, password=password, dbname=database)

with conn:
    cur = conn.cursor()
    cur.execute(score_drop)
    cur.execute(movie_drop)
    cur.execute(director_drop)
    cur.execute(star_drop)
    cur.execute(writer_drop)

    cur.execute(sequence_drop)
    cur.execute(sequence_create)
    cur.execute(director_create)

    with open(INPUT_CSV_FILE, 'r', encoding='utf-8') as inf:
        reader = csv.DictReader(inf)
        for row in reader:
            values = (
                row['director'],
            )
            cur.execute(director_insert, values)
            id_director = cur.fetchone()


conn.commit()
###
# star_drop = '''
# DROP TABLE IF EXISTS star
# '''
star_create = '''
CREATE TABLE star
(
  id_star INT DEFAULT nextval('star_id_star_seq'::regclass) PRIMARY KEY,
  name_s VARCHAR(50) UNIQUE NOT NULL
);
'''

star_insert = '''
INSERT INTO star (name_s)
VALUES (%s)
ON CONFLICT (name_s) DO NOTHING
RETURNING id_star;
'''

sequencestar_create = '''
CREATE SEQUENCE star_id_star_seq
START WITH 200001;
'''

sequencestar_drop ='''
DROP SEQUENCE IF EXISTS star_id_star_seq;
'''

connstar = psycopg2.connect(user=username, password=password, dbname=database)

with connstar:
    cur = connstar.cursor()
    # cur.execute(star_drop)
    cur.execute(sequencestar_drop)
    cur.execute(sequencestar_create)
    cur.execute(star_create)

    with open(INPUT_CSV_FILE, 'r', encoding='utf-8') as inf:
        reader = csv.DictReader(inf)
        for row in reader:
            values = (
                row['star'],
            )
            cur.execute(star_insert, values)
            id_star = cur.fetchone()

conn.commit()
###
# writer_drop = '''
# DROP TABLE IF EXISTS writer
# '''
writer_create = '''
CREATE TABLE writer
(
  id_writer INT DEFAULT nextval('writer_id_writer_seq'::regclass) PRIMARY KEY,
  name_w VARCHAR(50) UNIQUE NOT NULL
);
'''

writer_insert = '''
INSERT INTO writer (name_w)
VALUES (%s)
ON CONFLICT (name_w) DO NOTHING
RETURNING id_writer;
'''

sequencewriter_create = '''
CREATE SEQUENCE writer_id_writer_seq
START WITH 300001;
'''

sequencewriter_drop = '''
DROP SEQUENCE IF EXISTS writer_id_writer_seq;
'''

connwriter = psycopg2.connect(user=username, password=password, dbname=database)

with connwriter:
    cur = conn.cursor()
    # cur.execute(writer_drop)
    cur.execute(sequencewriter_drop)
    cur.execute(sequencewriter_create)
    cur.execute(writer_create)

    with open(INPUT_CSV_FILE, 'r', encoding='utf-8') as inf:
        reader = csv.DictReader(inf)
        for row in reader:
            values = (
                row['writer'],
            )
            cur.execute(writer_insert, values)
            id_writer = cur.fetchone()


conn.commit()

# 
# Define SQL statements for Movie table
# movie_drop = '''
# DROP TABLE IF EXISTS Movie
# '''

movie_create = '''
CREATE TABLE Movie
(
  id_movie INT DEFAULT nextval('moviee_id_movie_seq'::regclass) PRIMARY KEY,
  name_f VARCHAR(50) NOT NULL,
  genre VARCHAR(30) NOT NULL,
  country VARCHAR(30) NOT NULL,
  id_director INT,
  id_star INT,
  id_writer INT,
  FOREIGN KEY (id_director) REFERENCES Director(id_director),
  FOREIGN KEY (id_star) REFERENCES Star(id_star),
  FOREIGN KEY (id_writer) REFERENCES Writer(id_writer)
);
'''

movie_insert = '''
INSERT INTO Movie (name_f, genre, country, id_director, id_star, id_writer)
VALUES (%s, %s, %s, %s, %s, %s)
RETURNING id_movie;
'''
sequencemovie_create = '''
CREATE SEQUENCE moviee_id_movie_seq
START WITH 400001;
'''

sequencemovie_drop = '''
DROP SEQUENCE IF EXISTS moviee_id_movie_seq;
'''
connmovie = psycopg2.connect(user=username, password=password, dbname=database)

with connmovie:
    cur = connmovie.cursor()

    # Drop and create Movie table
    # cur.execute(movie_drop)
    cur.execute(sequencemovie_drop)
    cur.execute(sequencemovie_create)
    cur.execute(movie_create)

    # Open CSV file and insert data into Movie table
    with open(INPUT_CSV_FILE, 'r', encoding='utf-8') as inf:
        reader = csv.DictReader(inf)
        for row in reader:
            # Fetch id_director from Director table
            cur.execute("SELECT id_director FROM Director WHERE name_d = %s", (row['director'],))
            id_director = cur.fetchone()[0]  # Assuming 'name' is the column in the CSV file

            # Fetch id_star from Star table
            cur.execute("SELECT id_star FROM Star WHERE name_s = %s", (row['star'],))
            id_star = cur.fetchone()[0]  # Assuming 'star' is the column in the CSV file

            # Fetch id_writer from Writer table
            cur.execute("SELECT id_writer FROM Writer WHERE name_w = %s", (row['writer'],))
            id_writer = cur.fetchone()[0]  # Assuming 'writer' is the column in the CSV file

            values = (
                row['name'][:50],  # Truncate to 50 characters
                row['genre'],
                row['country'],
                id_director,
                id_star,
                id_writer,
            )
            cur.execute(movie_insert, values)
            id_movie = cur.fetchone()
            # print(f'Inserted row with id_movie: {id_movie}')

# Commit the changes
connmovie.commit()
###

# score_drop = '''
# DROP TABLE IF EXISTS score
# '''

score_create = '''
CREATE TABLE score
(
  id_movie INT DEFAULT nextval('movie_id_movie_seq'::regclass) PRIMARY KEY,
  score_num FLOAT NOT NULL,
  year_released INT NOT NULL
);
'''

score_insert = '''
INSERT INTO score (score_num, year_released)
VALUES (%s, %s)
RETURNING id_movie;
'''

sequencescore_create = '''
CREATE SEQUENCE movie_id_movie_seq
START WITH 400001;
'''

sequencescore_drop = '''
DROP SEQUENCE IF EXISTS movie_id_movie_seq;
'''

connscore = psycopg2.connect(user=username, password=password, dbname=database)

with connscore:
    cur = connscore.cursor()

    # Drop and create Score table
    # cur.execute(score_drop)
    cur.execute(sequencescore_drop)
    cur.execute(sequencescore_create)
    cur.execute(score_create)

    # Open CSV file and insert data into Score table
    with open(INPUT_CSV_FILE, 'r', encoding='utf-8') as inf:
        reader = csv.DictReader(inf)
        for row in reader:
            score_str = row['score']
            year_str = row['year']

            # Check if the strings are not empty before converting to float and int
            if score_str and year_str:
                values = (
                    float(score_str),
                    int(year_str),
                )
                cur.execute(score_insert, values)
                id_movie = cur.fetchone()

# Commit the changes
connscore.commit()


