package com.example.demo.DBRead;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgreSQLSystem extends DBSystem {
    private Connection conn;

    public PostgreSQLSystem() throws SQLException {
        super("postgres");
        String url = "jdbc:postgresql://localhost:5432/yourdb";
        String user = "youruser";
        String password = "yourpass";
        conn = DriverManager.getConnection(url, user, password);
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
}