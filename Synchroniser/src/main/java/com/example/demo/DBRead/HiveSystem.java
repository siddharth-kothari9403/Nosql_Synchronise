package com.example.demo.DBRead;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
class HiveSystem extends DBSystem {
    private Connection conn;

    public HiveSystem() throws SQLException {
        super("hive");
        String url = "jdbc:hive2://localhost:10000/default";
        conn = DriverManager.getConnection(url, "youruser", "yourpass");
    }

    @Override
    public String readGrade(String studentId) {
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT grade FROM grades WHERE student_id = '" + studentId + "'");
            if (rs.next()) return rs.getString("grade");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return "Not Found";
    }

    @Override
    public void updateGrade(String studentId, String grade) {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("INSERT OVERWRITE TABLE grades SELECT '" + studentId + "', '" + grade + "'");
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
