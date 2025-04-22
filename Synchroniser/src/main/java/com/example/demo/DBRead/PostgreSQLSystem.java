package com.example.demo.DBRead;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.opencsv.CSVReader;

@Configuration
@PropertySource("classpath:application.properties")
public class PostgreSQLSystem extends DBSystem {
    private Connection conn;

    private String csvFilePath = "./data/student_course_grades.csv";

    // @Value("${postgres.url}")
    // private String url;

    // @Value("${postgres.user}")
    // private String user;

    // @Value("${postgres.password}")
    // private String password;


    public PostgreSQLSystem() throws SQLException {
        super("postgres");
        // conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/student_course_grades", "myuser", "mypassword");
        conn = DriverManager.getConnection("jdbc:postgresql://host.docker.internal:5432/student_course_grades", "myuser", "mypassword");
    }

    @Override
    public String readGrade(String studentId, String courseId) {
        try {
            PreparedStatement stmt = conn.prepareStatement("SELECT grade FROM grades WHERE student_id = ? AND course_id = ?");
            stmt.setString(1, studentId);
            stmt.setString(2, courseId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getString("grade");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return "Not Found";
    }

    @Override
    public void updateGrade(String studentId,String couseId,String grade) {
        try {
            PreparedStatement stmt = conn.prepareStatement("INSERT INTO grades(student_id, course_id, grade) VALUES(?, ?, ?) ON CONFLICT (student_id, course_id) DO UPDATE SET grade = ?");
            stmt.setString(1, studentId);
            stmt.setString(2, couseId);
            stmt.setString(3, grade);
            stmt.setString(4, grade);
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