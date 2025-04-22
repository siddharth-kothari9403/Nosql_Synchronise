CREATE TABLE IF NOT EXISTS student_grades (
    student_id STRING,
    course_id STRING,
    roll_no STRING,
    email_id STRING,
    grade STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

-- Step 2: Load data from CSV (update the path accordingly)
LOAD DATA INPATH '/opt/hive/mydata/student_course_grades.csv' INTO TABLE student_grades;