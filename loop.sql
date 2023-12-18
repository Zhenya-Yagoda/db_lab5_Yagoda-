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

delete from score;

DO $$
DECLARE
    movie_id     INT;
    year_released INT;
    score_num     FLOAT;
BEGIN
	FOR movie_id IN 4000001..4000008
    LOOP
        CASE movie_id
            WHEN 4000001 THEN
                year_released := '1980';
                score_num := 8.4;
            WHEN 4000002 THEN
                year_released := '1980';
                score_num := 5.8;
            WHEN 4000003 THEN
                year_released := '1980';
                score_num := 8.7;
            WHEN 4000004 THEN
                year_released := '1980';
                score_num := 7.7;
            WHEN 4000005 THEN
                year_released := '1980';
                score_num := 7.3;
            WHEN 4000006 THEN
                year_released := '1987';
                score_num := 8.3;
            WHEN 4000007 THEN
                year_released := '1982';
                score_num := 6.9;
            WHEN 4000008 THEN
                year_released := '1983';
                score_num := 6.1;
        END CASE;

        INSERT INTO score (year_released, score_num, id_movie)
        VALUES (year_released, score_num, movie_id);
    END LOOP;
END;
$$;
