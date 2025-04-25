package com.example.demo.DBRead;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.*;
import java.util.List;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public abstract class DBSystem {
    protected String name;

    public DBSystem(String name) {
        this.name = name;
    }

    // Read grade for a given student ID
    public abstract String readGrade(String studentId, String courseId);

    // Update grade for a given student ID
    public abstract void updateGrade(String studentId, String courseId, String grade);

    // Merge updates from another system based on oplogs
    public abstract void merge(String fromSystem);

    public static int compareTimestamps(String timestamp1, String timestamp2) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");

        // Parse the timestamps into LocalDateTime objects
        LocalDateTime time1 = LocalDateTime.parse(timestamp1, formatter);
        LocalDateTime time2 = LocalDateTime.parse(timestamp2, formatter);

        // Compare the timestamps
        return time1.compareTo(time2);
    }

    // Log an operation to this system’s oplog
    protected void logOperation(String opType, String studentId, String courseId, String value) {
        Operation op = new Operation(opType, studentId, courseId,value);
        System.out.printf("[%s] Logged operation: %s%n", name.toUpperCase(), op);
    }

    protected void writeToLogFile(String message, String logfile) {
        try {
            Path logPath = Paths.get("src/main/resources/" + logfile);
            Files.write(
                    logPath,
                    (java.time.LocalDateTime.now() + " - " + message + System.lineSeparator()).getBytes(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    protected void writeToLogFile(String message, String logfile, String timestamp) {
        try {
            Path logPath = Paths.get("src/main/resources/" + logfile);
            Files.write(
                    logPath,
                    (timestamp + " - " + message + System.lineSeparator()).getBytes(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public String getLatestUpdateTimestamp(String studentId, String courseId) {
        Path logPath = Paths.get("src/main/resources/" + name.toLowerCase() + "-log.txt");
        String latestTimestamp = null;

        try {
            List<String> lines = Files.readAllLines(logPath);
            for (String line : lines) {
                if (line.contains("UPDATE") && line.contains("studentId=" + studentId) && line.contains("courseId=" + courseId)) {
                    String[] parts = line.split(" - ");
                    latestTimestamp = parts[0].trim(); // Extract the timestamp
                }
            }
        } catch (IOException e) {
            System.out.println("Error reading log file: " + getStackTrace(e));
        }

        return latestTimestamp;
    }

    protected String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

}
