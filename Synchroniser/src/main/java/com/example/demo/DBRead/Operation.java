package com.example.demo.DBRead;
public class Operation {
    String opType;       // "update", "merge_update", etc.
    String studentId;
    String value;

    public Operation(String opType, String studentId, String value) {
        this.opType = opType;
        this.studentId = studentId;
        this.value = value;
    }

    @Override
    public String toString() {
        return String.format("Operation(opType=%s, studentId=%s, value=%s)", opType, studentId, value);
    }
}
