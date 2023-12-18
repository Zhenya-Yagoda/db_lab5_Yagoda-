-- drop table score;
-- drop table movie;
-- drop table director;
-- drop table star;
-- drop table writer;

-- select * from director;
-- select * from movie;
-- select * from score;
-- select * from star;
-- select * from writer;

CREATE TABLE Director
(
  id_director INT,
  name_d VARCHAR(50) NOT NULL,
  PRIMARY KEY (id_director)
);

CREATE TABLE Star
(
  id_star INT,
  name_s VARCHAR(50) NOT NULL,
  PRIMARY KEY (id_star)
);

CREATE TABLE Writer
(
  id_writer INT,
  name_w VARCHAR(50) NOT NULL,
  PRIMARY KEY (id_writer)
);

CREATE TABLE Movie
(
  id_movie INT,
  name_f VARCHAR(50) NOT NULL,
  genre VARCHAR(30) NOT NULL,
  country VARCHAR(30) NOT NULL,
  id_director INT NOT NULL,
  id_star INT NOT NULL,
  id_writer INT NOT NULL,
  PRIMARY KEY (id_movie),
  FOREIGN KEY (id_director) REFERENCES Director(id_director),
  FOREIGN KEY (id_star) REFERENCES Star(id_star),
  FOREIGN KEY (id_writer) REFERENCES Writer(id_writer)
);

CREATE TABLE Score
(
  score_num FLOAT NOT NULL,
  year_released INT NOT NULL,
  id_movie INT ,
  FOREIGN KEY (id_movie) REFERENCES Movie(id_movie)
);