package com.example.demo.ImportCSVs;

import com.opencsv.CSVReader;

import java.io.FileReader;
import java.sql.*;

public class CsvToPostgresImporter {

    public static void importFile() {
        String csvFilePath = "./data/student_course_grades.csv";
        String url = "jdbc:postgresql://localhost:5432/student_course_grades";
        String user = "postgres";
        String password = "postgres";

        try (
                Connection conn = DriverManager.getConnection(url, user, password);
                CSVReader reader = new CSVReader(new FileReader(csvFilePath))
        ) {
            String[] headers = reader.readNext(); // Skip header row
            if (headers == null) {
                System.out.println("CSV file is empty.");
                return;
            }

            String insertSQL = "INSERT INTO student_grades (student_id, course_id, roll_no, email_id, grade) VALUES (?, ?, ?, ?, ?)";
            PreparedStatement stmt = conn.prepareStatement(insertSQL);

            String[] line;
            int count = 0;

            while ((line = reader.readNext()) != null) {
                stmt.setString(1, line[0]);
                stmt.setString(2, line[1]);
                stmt.setString(3, line[2]);
                stmt.setString(4, line[3]);
                stmt.setString(5, line[4]);
                stmt.addBatch();
                count++;

                if (count % 100 == 0) {
                    stmt.executeBatch();
                }
            }

            stmt.executeBatch(); // Final batch
            System.out.println("Inserted " + count + " rows into PostgreSQL.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}