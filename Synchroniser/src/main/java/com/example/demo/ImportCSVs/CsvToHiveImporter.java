package com.example.demo.ImportCSVs;
import com.opencsv.CSVReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;

import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Configuration
@PropertySource("classpath:application.properties")
public class CsvToHiveImporter {

    @Value("${csv.file.path}")
    private String csvFilePath;

    @Value("${hive.url}")
    private String url;

    @Value("${hive.user}")
    private String user;

    @Value("${hive.password}")
    private String password;

    public DataSource getHiveDataSource() {

        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUrl(url);
        dataSource.setDriverClassName("org.apache.hive.jdbc.HiveDriver");
        dataSource.setUsername(user);
        dataSource.setPassword(password);

        return dataSource;
    }

    public JdbcTemplate getJDBCTemplate() throws IOException {
        return new JdbcTemplate(getHiveDataSource());
    }

    public void importFile() throws IOException {
        JdbcTemplate jdbcTemplate = getJDBCTemplate();
        try (
                CSVReader reader = new CSVReader(new FileReader(csvFilePath));
                Connection conn = Objects.requireNonNull(jdbcTemplate.getDataSource()).getConnection()
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

            // Prepare data for batch insert
            List<Object[]> batchArgs = new ArrayList<>();
            String[] line;
            int count = 0;

            while ((line = reader.readNext()) != null) {
                // Add data to batch (use Object[] to match JdbcTemplate batchUpdate requirement)
                batchArgs.add(new Object[]{line[0], line[1], line[2], line[3], line[4]});
                count++;

                if (count % 100 == 0) {
                    jdbcTemplate.batchUpdate(insertSQL, batchArgs);
                    batchArgs.clear();  // Clear batch arguments after each batch execution
                    System.out.println("Inserted " + count + " rows into Hive.");
                }
            }

            // Execute the final batch
            if (!batchArgs.isEmpty()) {
                jdbcTemplate.batchUpdate(insertSQL, batchArgs);
            }

            System.out.println("Inserted " + count + " rows into Hive.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
