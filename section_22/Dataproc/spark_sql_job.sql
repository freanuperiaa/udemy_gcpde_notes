CREATE OR REPLACE TEMP VIEW data (id,name,age,city) AS
SELECT * FROM csv.`${gcs_path}`;
SELECT * FROM data WHERE age > 30;
SELECT id, upper(name) AS upper_name, age, city FROM data;
SELECT id, name, sqrt(age) AS age_sqrt, city FROM data;