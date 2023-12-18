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

---------------------------
-- Populate director table
---------------------------
INSERT INTO director(id_director,name_d)
VALUES('1000001', 'Stanley Kubrick');
INSERT INTO director(id_director,name_d)
VALUES('1000002', 'Randal Kleiser');
INSERT INTO director(id_director,name_d)
VALUES('1000003', 'Irvin Kershner');
INSERT INTO director(id_director,name_d)
VALUES('1000004', 'Jim Abrahams');
INSERT INTO director(id_director,name_d)
VALUES('1000005', 'Harold Ramis');
INSERT INTO director(id_director,name_d)
VALUES('1000006', 'George A. Romero');
INSERT INTO director(id_director,name_d)
VALUES('1000007', 'Lewis Teague');
--------------------------
-- Populate star table
--------------------------

INSERT INTO star(id_star,name_s)
VALUES('2000001', 'Jack Nicholson');
INSERT INTO star(id_star,name_s)
VALUES('2000002', 'Brooke Shields');
INSERT INTO star(id_star,name_s)
VALUES('2000003', 'Mark Hamill');
INSERT INTO star(id_star,name_s)
VALUES('2000004', 'Robert Hays');
INSERT INTO star(id_star,name_s)
VALUES('2000005', 'Chevy Chase');
INSERT INTO star(id_star,name_s)
VALUES('2000006', 'Matthew Modine');
INSERT INTO star(id_star,name_s)
VALUES('2000007', 'Hal Holbrook');
INSERT INTO star(id_star,name_s)
VALUES('2000008', 'Dee Wallace');




------------------------
-- Populate writer table
------------------------

INSERT INTO writer(id_writer,name_w)
VALUES('3000001', 'Stephen King');
INSERT INTO writer(id_writer,name_w)
VALUES('3000002', 'Henry De Vere Stacpoole');
INSERT INTO writer(id_writer,name_w)
VALUES('3000003', 'Leigh Brackett');
INSERT INTO writer(id_writer,name_w)
VALUES('3000004', 'Jim Abrahams');
INSERT INTO writer(id_writer,name_w)
VALUES('3000005', 'Brian Doyle-Murray');
INSERT INTO writer(id_writer,name_w)
VALUES('3000006', 'Stanley Kubrick');


-------------------------
-- Populate movie table
-------------------------
INSERT INTO movie(id_movie, name_f, genre, country,id_director,id_star,id_writer)
VALUES('4000001', 'The Shining',  'Drama',   'United Kingdom','1000001','2000001','3000001');
INSERT INTO movie(id_movie, name_f, genre, country,id_director,id_star,id_writer)
VALUES('4000002', 'The Blue Lagoon',  'Adventure',   'United States','1000002','2000002','3000002');
INSERT INTO movie(id_movie, name_f, genre, country,id_director,id_star,id_writer)
VALUES('4000003', 'Star Wars: Episode V - The Empire Strikes Back', 'Action',  'United States','1000003','2000003','3000003');
INSERT INTO movie(id_movie, name_f, genre,  country,id_director,id_star,id_writer)
VALUES('4000004', 'Airplane!',  'Comedy',  'United States','1000004','2000004','3000004');
INSERT INTO movie(id_movie, name_f, genre,country,id_director,id_star,id_writer)
VALUES('4000005', 'Caddyshack',  'Comedy',  'United States','1000005','2000005','3000005');

INSERT INTO movie(id_movie, name_f, genre,  country,id_director,id_star,id_writer)
VALUES('4000006', 'Full Metal Jacket',  'Drama',   'United Kingdom','1000001','2000006','3000006');
INSERT INTO movie(id_movie, name_f, genre,  country,id_director,id_star,id_writer)
VALUES('4000007', 'Creepshow',  'Comedy', 'United States', '1000006', '2000007','3000001');
INSERT INTO movie(id_movie, name_f, genre,  country,id_director,id_star,id_writer)
VALUES('4000008', 'Cujo', 'Horror',   'United States', '1000007', '2000008','3000001');

----------------------------
-- Populate score table
----------------------------
INSERT INTO score(year_released,score_num,id_movie)
VALUES('1980', '8.4','4000001');
INSERT INTO score(year_released,score_num,id_movie)
VALUES('1980', '5.8','4000002');
INSERT INTO score(year_released,score_num,id_movie)
VALUES('1980', '8.7','4000003');
INSERT INTO score(year_released,score_num,id_movie)
VALUES('1980', '7.7','4000004');
INSERT INTO score(year_released,score_num,id_movie)
VALUES('1980', '7.3','4000005');
INSERT INTO score(year_released,score_num,id_movie)
VALUES('1987', '8.3','4000006');
INSERT INTO score(year_released,score_num,id_movie)
VALUES('1982', '6.9','4000007');
INSERT INTO score(year_released,score_num,id_movie)
VALUES('1983', '6.1','4000008');
