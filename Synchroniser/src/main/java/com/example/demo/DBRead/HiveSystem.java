package com.example.demo.DBRead;

import com.opencsv.CSVReader;
import jakarta.annotation.PostConstruct;
import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.sql.SQLException;
import java.util.ArrayList;

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

    Path tempCsvFile = Paths.get("data/student_grades_noheader.csv").toAbsolutePath();

    @Override
    public String readGrade(String studentId, String courseId) {
        try {
            String sql = "SELECT grade FROM grades WHERE student_id = ? AND course_id = ?";
            return jdbcTemplate.queryForObject(sql, new Object[]{studentId, courseId}, String.class);
        } catch (Exception e) {
            e.printStackTrace();
            return "Not Found";
        }
    }

    @Override
    public void updateGrade(String studentId, String courseId, String grade) {
        try {
            // Try to update existing record
            String updateSQL = "UPDATE grades SET grade = ? WHERE student_id = ? AND course_id = ?";
            int updated = jdbcTemplate.update(updateSQL, grade, studentId, courseId);

            // If no rows were updated, insert a new one
            if (updated == 0) {
                String insertSQL = "INSERT INTO grades(student_id, course_id, grade) VALUES(?, ?, ?)";
                jdbcTemplate.update(insertSQL, studentId, courseId, grade);
            }

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
        try (
                CSVReader reader = new CSVReader(new FileReader(csvFilePath));
                BufferedWriter writer = Files.newBufferedWriter(tempCsvFile);
        ) {
            // Drop the table if it exists
            String dropTableSQL = "DROP TABLE IF EXISTS student_grades";
            jdbcTemplate.execute(dropTableSQL);
            System.out.println("Dropped existing table (if any).");

            // Create the table
            String createTableSQL = "CREATE TABLE student_grades (" +
                    "student_id STRING, " +
                    "course_id STRING, " +
                    "roll_no STRING, " +
                    "email_id STRING, " +
                    "grade STRING" +
                    ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE";
            jdbcTemplate.execute(createTableSQL);
            System.out.println("Created new table.");

            // Skip header row
            String[] headers = reader.readNext();
            if (headers == null) {
                System.out.println("CSV file is empty.");
                return;
            }

            // Write the remaining rows to the new temporary CSV file
            String[] line;
            int rowCount = 0;
            while ((line = reader.readNext()) != null) {
                writer.write(String.join(",", line));
                writer.newLine();
                rowCount++;
            }

            // Load data from the new CSV (without header)
            String loadDataSQL = "LOAD DATA LOCAL INPATH '/opt/hive/mydata/student_grades_noheader.csv' INTO TABLE student_grades";
            jdbcTemplate.execute(loadDataSQL);
            System.out.println("Loaded CSV data into Hive.");

            // Clean up temporary file
            Files.deleteIfExists(tempCsvFile);

            System.out.println("Inserted " + rowCount + " rows into Hive.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
