package com.example.demo.DBRead;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class DBSystem {
    protected String name;

    // Operation log: each system keeps a map of other systems' oplogs
    protected static Map<String, List<Operation>> oplogs = new HashMap<>();

    public DBSystem(String name) {
        this.name = name;
        if (!oplogs.containsKey(name)) {
            oplogs.put(name, new ArrayList<>());
        }
    }

    // Read grade for a given student ID
    public abstract String readGrade(String studentId, String courseId);

    // Update grade for a given student ID
    public abstract void updateGrade(String studentId, String courseId, String grade);

    // Merge updates from another system based on oplogs
    public abstract void merge(String fromSystem);

    // Log an operation to this system’s oplog
    protected void logOperation(String opType, String studentId, String courseId, String value) {
        Operation op = new Operation(opType, studentId, courseId,value);
        oplogs.get(name).add(op);
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


}
