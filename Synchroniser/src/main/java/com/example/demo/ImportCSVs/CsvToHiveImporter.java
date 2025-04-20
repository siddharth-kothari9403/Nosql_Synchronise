package com.example.demo.ImportCSVs;
import com.opencsv.CSVReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.io.FileReader;
import java.sql.*;

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

    public void importFile() {

        try (
                Connection conn = DriverManager.getConnection(url, user, password);
                CSVReader reader = new CSVReader(new FileReader(csvFilePath))
        ) {
            // Drop the table if it exists
            String dropTableSQL = "DROP TABLE IF EXISTS student_grades";
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(dropTableSQL);
                System.out.println("Dropped existing table (if any).");
            }

            // Create the table
            String createTableSQL = "CREATE TABLE student_grades (" +
                    "student_id STRING, " +
                    "course_id STRING, " +
                    "roll_no STRING, " +
                    "email_id STRING, " +
                    "grade STRING" +
                    ") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE";
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(createTableSQL);
                System.out.println("Created new table.");
            }

            // Read CSV and insert data into Hive
            String[] headers = reader.readNext(); // Skip header row
            if (headers == null) {
                System.out.println("CSV file is empty.");
                return;
            }

            String insertSQL = "INSERT INTO TABLE student_grades VALUES (?, ?, ?, ?, ?)";
            PreparedStatement preparedStatement = conn.prepareStatement(insertSQL);

            String[] line;
            int count = 0;

            while ((line = reader.readNext()) != null) {
                preparedStatement.setString(1, line[0]);
                preparedStatement.setString(2, line[1]);
                preparedStatement.setString(3, line[2]);
                preparedStatement.setString(4, line[3]);
                preparedStatement.setString(5, line[4]);
                preparedStatement.addBatch();
                count++;

                if (count % 100 == 0) {
                    preparedStatement.executeBatch();
                }
            }

            preparedStatement.executeBatch(); // Final batch
            System.out.println("Inserted " + count + " rows into Hive.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
