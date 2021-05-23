DROP TABLE IF EXISTS login;
DROP TABLE IF EXISTS logout;

CREATE TABLE login (
	start_time	timestamp,
	name	text,
	session_id	int
);

CREATE TABLE logout (
	end_time	timestamp,
	name	text,
	session_id	int
);	
	
INSERT INTO login VALUES ('2021-05-01 04:00:01.638643+05','USER1',1);
INSERT INTO login VALUES ('2021-05-01 04:03:01.638643+05','USER2',2);
INSERT INTO login VALUES ('2021-05-02 06:00:00.638643+05','USER1',1);
INSERT INTO login VALUES ('2021-05-02 06:10:01.638643+05','USER2',2);
INSERT INTO login VALUES ('2021-05-03 06:00:00.638643+05','USER1',1);
INSERT INTO login VALUES ('2021-05-03 06:00:00.638643+05','USER2',2);
INSERT INTO login VALUES ('2021-05-03 08:00:00.638643+05','USER1',1);
INSERT INTO login VALUES ('2021-05-03 09:00:00.638643+05','USER2',2);

INSERT INTO logout VALUES ('2021-05-01 04:01:01.638643+05','USER1',1);
INSERT INTO logout VALUES ('2021-05-01 04:05:01.638643+05','USER2',2);
INSERT INTO logout VALUES ('2021-05-02 06:10:00.638643+05','USER1',1);
INSERT INTO logout VALUES ('2021-05-02 06:20:01.638643+05','USER2',2);
INSERT INTO logout VALUES ('2021-05-03 07:00:00.638643+05','USER1',1);
INSERT INTO logout VALUES ('2021-05-03 08:00:00.638643+05','USER2',2);
INSERT INTO logout VALUES ('2021-05-04 08:00:00.638643+05','USER1',1);
INSERT INTO logout VALUES ('2021-05-05 09:00:00.638643+05','USER2',2);





SELECT * FROM login;
SELECT * FROM logout;


# Правильно формируем длительность сессий.
SELECT
	start_time,
	start_name,
	start_session_id,
	(end_time - start_time) as duration,
	end_time,
	end_name,
	end_session_id
FROM
	(SELECT 
		t1.start_time,
		t1.name as start_name,
	 	t1.session_id as start_session_id,
	 	(t2.end_time - t1.start_time) as duration,
	 	t2.end_time,
		t2.name as end_name,
		t2.session_id as end_session_id,
	 	ROW_NUMBER() OVER(PARTITION BY t1.start_time, t1.name ORDER BY end_time) AS rang
	FROM login t1
	INNER JOIN logout t2
	ON t1.session_id = t2.session_id
	AND t1.start_time < t2.end_time) sub_req
WHERE rang = 1






