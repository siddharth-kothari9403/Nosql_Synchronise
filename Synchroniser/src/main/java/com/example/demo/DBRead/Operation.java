package com.example.demo.DBRead;

public class Operation {
    String opType;       // "update", "merge_update", etc.
    String studentId;
    String value;
    String courseId;

    public Operation(String opType, String studentId, String courseId,String value) {
        this.opType = opType;
        this.studentId = studentId;
        this.value = value;
        this.courseId = courseId;
    }

    @Override
    public String toString() {
        return String.format("Operation(opType=%s, courseId=%s ,studentId=%s, value=%s)", opType, courseId,studentId, value);
    }
}
