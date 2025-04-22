package com.example.demo.ImportCSVs;

import com.opencsv.CSVReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.io.FileReader;
import java.sql.*;

@Configuration
@PropertySource("classpath:application.properties")
public class CsvToPostgresImporter {

    @Value("${csv.file.path}")
    private String csvFilePath;

    @Value("${postgres.url}")
    private String url;

    @Value("${postgres.user}")
    private String user;

    @Value("${postgres.password}")
    private String password;

    public void importFile() {

        try (
                Connection conn = DriverManager.getConnection(url, user, password);
                CSVReader reader = new CSVReader(new FileReader(csvFilePath))
        ) {
            Statement stmtDDL = conn.createStatement();

            // Drop the table if it exists
            stmtDDL.executeUpdate("DROP TABLE IF EXISTS student_grades");

            // Create the table
            stmtDDL.executeUpdate(
                    "CREATE TABLE student_grades (" +
                            "student_id VARCHAR(50), " +
                            "course_id VARCHAR(50), " +
                            "roll_no VARCHAR(50), " +
                            "email_id VARCHAR(100), " +
                            "grade VARCHAR(5))"
            );

            String[] headers = reader.readNext(); // Skip header
            if (headers == null) {
                System.out.println("CSV file is empty.");
                return;
            }

            String insertSQL = "INSERT INTO student_grades (student_id, course_id, roll_no, email_id, grade) VALUES (?, ?, ?, ?, ?)";
            PreparedStatement stmtInsert = conn.prepareStatement(insertSQL);

            String[] line;
            int count = 0;

            while ((line = reader.readNext()) != null) {
                stmtInsert.setString(1, line[0]);
                stmtInsert.setString(2, line[1]);
                stmtInsert.setString(3, line[2]);
                stmtInsert.setString(4, line[3]);
                stmtInsert.setString(5, line[4]);
                stmtInsert.addBatch();
                count++;

                if (count % 100 == 0) {
                    stmtInsert.executeBatch();
                }
            }

            stmtInsert.executeBatch(); // Final batch
            System.out.println("Inserted " + count + " rows into PostgreSQL.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
