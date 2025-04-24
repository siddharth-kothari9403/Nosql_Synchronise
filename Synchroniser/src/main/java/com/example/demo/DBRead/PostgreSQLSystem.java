package com.example.demo.DBRead;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.opencsv.CSVReader;

import jakarta.annotation.PostConstruct;

@Configuration
@PropertySource("classpath:application.properties")
public class PostgreSQLSystem extends DBSystem {
    private Connection conn;

    @Value("${csv.file.path}")
    private String csvFilePath;

     @Value("${postgres.url}")
     private String url;

     @Value("${postgres.user}")
     private String user;

     @Value("${postgres.password}")
     private String password;

     public PostgreSQLSystem() throws SQLException {
         super("postgres");
     }

    @PostConstruct
    public void initPostgres() throws SQLException {
         conn = DriverManager.getConnection(url, user, password);
//        conn = DriverManager.getConnection("jdbc:postgresql://host.docker.internal:5432/student_course_grades", user, password);
    }

    @Override
    public String readGrade(String studentId, String courseId) {
        String returnString = "Not Found";
        try {
            PreparedStatement stmt = conn.prepareStatement("SELECT grade FROM grades WHERE student_id = ? AND course_id = ?");
            stmt.setString(1, studentId);
            stmt.setString(2, courseId);
            ResultSet rs = stmt.executeQuery();


            if (rs.next()) {
                returnString = rs.getString("grade");
                logOperation("read", studentId, courseId, returnString);
            }else{
                logOperation("read", studentId, courseId, "Not Found");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return returnString;
    }

    @Override
    public void updateGrade(String studentId,String couseId,String grade) {
        try {
            PreparedStatement stmt = conn.prepareStatement("UPDATE grades SET grade = ? WHERE student_id = ? AND course_id = ?");
            stmt.setString(1, grade);
            stmt.setString(2, studentId);
            stmt.setString(3, couseId);
            stmt.executeUpdate();
            logOperation("update", studentId, couseId, grade);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void merge(String fromSystem) {
        for (Operation op : oplogs.getOrDefault(fromSystem, new ArrayList<>())) {
            if (op.opType.equals("update")) {
                updateGrade(op.studentId,op.courseId,op.value);
                logOperation("merge_update", op.studentId, op.courseId,op.value);
            }
        }
    }

    public void importFile() {

        try (
                CSVReader reader = new CSVReader(new FileReader(csvFilePath))
        ) {
            Statement stmtDDL = conn.createStatement();

        // Drop the table if it exists
        stmtDDL.executeUpdate("DROP TABLE IF EXISTS grades");

        // Create the table
        stmtDDL.executeUpdate(
            "CREATE TABLE grades (" +
                "student_id VARCHAR(50), " +
                "course_id VARCHAR(50), " +
                "roll_no VARCHAR(50), " +
                "email_id VARCHAR(100), " +
                "grade VARCHAR(5))"
        );

        // Add unique constraint for ON CONFLICT clause
        stmtDDL.executeUpdate(
            "ALTER TABLE grades ADD CONSTRAINT unique_student_course UNIQUE (student_id, course_id)"
        );

        // Skip header
        String[] headers = reader.readNext();
        if (headers == null) {
            System.out.println("CSV file is empty.");
            return;
        }

        String insertSQL = "INSERT INTO grades (student_id, course_id, roll_no, email_id, grade) VALUES (?, ?, ?, ?, ?)";
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