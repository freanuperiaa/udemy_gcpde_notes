CREATE OR REPLACE TEMP VIEW data
(id,name,age,city) AS 
SELECT * FROM csv.`${gcs_path}`;
SELECT * FROM data WHERE age>=28;