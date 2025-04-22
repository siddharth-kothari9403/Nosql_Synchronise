package com.example.demo.DBRead;

import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import javax.sql.DataSource;

import com.opencsv.CSVReader;

public class HiveSystem extends DBSystem {
    
    private JdbcTemplate jdbcTemplate;
    private final String csvFilePath = "./data/student_course_grades.csv";

    public HiveSystem() throws SQLException {
        super("hive");
        this.jdbcTemplate = new JdbcTemplate(getHiveDataSource());
    }

    public DataSource getHiveDataSource() {
        BasicDataSource dataSource = new BasicDataSource();
        String url = "jdbc:hive2://localhost:10000/default";
        dataSource.setUrl(url);
        dataSource.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
        return dataSource;
    }

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
            CSVReader reader = new CSVReader(new FileReader(csvFilePath))
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

            // Prepare batch insert
            String insertSQL = "INSERT INTO TABLE student_grades VALUES (?, ?, ?, ?, ?)";
            List<Object[]> batchArgs = new ArrayList<>();
            String[] line;
            int count = 0;

            while ((line = reader.readNext()) != null) {
                batchArgs.add(new Object[]{line[0], line[1], line[2], line[3], line[4]});
                count++;

                if (count % 100 == 0) {
                    jdbcTemplate.batchUpdate(insertSQL, batchArgs);
                    batchArgs.clear();
                    System.out.println("Inserted " + count + " rows into Hive.");
                }
            }

            if (!batchArgs.isEmpty()) {
                jdbcTemplate.batchUpdate(insertSQL, batchArgs);
            }

            System.out.println("Inserted total " + count + " rows into Hive.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
