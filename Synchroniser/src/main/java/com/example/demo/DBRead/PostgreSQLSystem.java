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
    public String readGrade(String studentId) {
        try {
            PreparedStatement stmt = conn.prepareStatement("SELECT grade FROM grades WHERE student_id = ?");
            stmt.setString(1, studentId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) return rs.getString("grade");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return "Not Found";
    }

    @Override
    public void updateGrade(String studentId, String grade) {
        try {
            PreparedStatement stmt = conn.prepareStatement(
                "INSERT INTO grades(student_id, grade) VALUES(?, ?) ON CONFLICT (student_id) DO UPDATE SET grade = EXCLUDED.grade");
            stmt.setString(1, studentId);
            stmt.setString(2, grade);
            stmt.executeUpdate();
            logOperation("update", studentId, grade);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void merge(String fromSystem) {
        for (Operation op : oplogs.getOrDefault(fromSystem, new ArrayList<>())) {
            if (op.opType.equals("update")) {
                updateGrade(op.studentId, op.value);
                logOperation("merge_update", op.studentId, op.value);
            }
        }
    }
}