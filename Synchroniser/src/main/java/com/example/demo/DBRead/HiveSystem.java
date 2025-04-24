package com.example.demo.DBRead;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;

import jakarta.annotation.PostConstruct;

@Configuration
@PropertySource("classpath:application.properties")
public class HiveSystem extends DBSystem {

    @Value("${csv.file.path}")
    private String csvFilePath;

    @Value("${hive.url}")
    private String url;

    private JdbcTemplate jdbcTemplate;

    public DataSource getHiveDataSource() {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(url);
        dataSource.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
        return dataSource;
    }

    public HiveSystem() throws SQLException {
        super("hive");
    }

    @PostConstruct
    public void initHive(){
        this.jdbcTemplate = new JdbcTemplate(getHiveDataSource());
    }

    // Path tempCsvFile = Paths.get("data/student_grades_noheader.csv").toAbsolutePath();

    @Override
    public String readGrade(String studentId, String courseId) {
        String sql = "SELECT grade FROM grades WHERE student_id = ? AND course_id = ?";
        List<String> results = jdbcTemplate.query(
                sql,
                new Object[]{studentId, courseId},
                (rs, rowNum) -> rs.getString("grade")
        );

        String returnString = results.isEmpty() ? "Not Found" : results.get(0);
        logOperation("read", studentId, courseId, returnString);
        return returnString;
    }

    @Override
    public void updateGrade(String studentId, String courseId, String grade) {
        try {
            // Try to update existing record
            String updateSQL = "UPDATE grades SET grade = ? WHERE student_id = ? AND course_id = ?";
            jdbcTemplate.update(updateSQL, grade, studentId, courseId);

            // String updateSQL = "UPDATE grades SET grade = ? WHERE student_id = ? AND course_id = ?";
            // int updated = jdbcTemplate.update(updateSQL, grade, studentId, courseId);

            // If no rows were updated, insert a new one
            // if (updated == 0) {
            //     String insertSQL = "INSERT INTO grades(student_id, course_id, grade) VALUES(?, ?, ?)";
            //     jdbcTemplate.update(insertSQL, studentId, courseId, grade);
            // }

            logOperation("update", studentId, courseId, grade);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void merge(String fromSystem) {
        for (Operation op : oplogs.getOrDefault(fromSystem, new ArrayList<>())) {
            if (op.opType.equals("update")) {
                updateGrade(op.studentId, op.courseId, op.value);
                logOperation("merge_update", op.studentId, op.courseId, op.value);
            }
        }
    }


    public void importFile() throws IOException {
        try {
            // Drop the table if it exists
            // Drop the main ORC table if it exists
                String dropMainTableSQL = "DROP TABLE IF EXISTS grades";
                jdbcTemplate.execute(dropMainTableSQL);
                System.out.println("Dropped existing ORC table (if any).");

                // Drop the temporary CSV TEXTFILE table if it exists
                String dropTempTableSQL = "DROP TABLE IF EXISTS temp_grades";
                jdbcTemplate.execute(dropTempTableSQL);
                System.out.println("Dropped existing temporary CSV table (if any).");

                // Create the temporary table to load CSV data (stored as TEXTFILE)
                String createTempTableSQL = "CREATE TABLE temp_grades (" +
                        "student_id STRING, " +
                        "course_id STRING, " +
                        "roll_no STRING, " +
                        "email_id STRING, " +
                        "grade STRING" +
                        ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE";
                jdbcTemplate.execute(createTempTableSQL);
                System.out.println("Created temporary TEXTFILE table.");

                // Set necessary Hive settings for transactional ORC tables
                jdbcTemplate.execute("SET hive.support.concurrency = true");
                jdbcTemplate.execute("SET hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
                jdbcTemplate.execute("SET hive.enforce.bucketing = true");
                jdbcTemplate.execute("SET hive.exec.dynamic.partition.mode = nonstrict");
                jdbcTemplate.execute("SET hive.create.as.acid=true");
                jdbcTemplate.execute("SET hive.compactor.worker.threads=1");
                jdbcTemplate.execute("SET hive.compactor.initiator.on=true");

                // Create the main transactional ORC table
                String createMainTableSQL = "CREATE TABLE grades (" +
                        "student_id STRING, " +
                        "course_id STRING, " +
                        "roll_no STRING, " +
                        "email_id STRING, " +
                        "grade STRING" +
                        ") " +
                        "CLUSTERED BY (student_id) INTO 2 BUCKETS " +
                        "STORED AS ORC " +
                        "TBLPROPERTIES ('transactional'='true')";
                jdbcTemplate.execute(createMainTableSQL);
                System.out.println("Created transactional ORC table.");

                // Load the CSV data into the temporary TEXTFILE table
                String loadCSVSQL = "LOAD DATA LOCAL INPATH '/opt/hive/mydata/student_grades_noheader.csv' INTO TABLE temp_grades";
                jdbcTemplate.execute(loadCSVSQL);
                System.out.println("Loaded CSV data into temporary table.");

                // Insert the data from the TEXTFILE table into the ORC table
                String insertSQL = "INSERT INTO TABLE grades SELECT * FROM temp_grades";
                jdbcTemplate.execute(insertSQL);
                System.out.println("Inserted data from temporary table into transactional ORC table.");


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}