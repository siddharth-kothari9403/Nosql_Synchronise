package com.example.demo;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import com.example.demo.DBRead.HiveSystem;
import com.example.demo.DBRead.MongoDBSystem;
import com.example.demo.DBRead.PostgreSQLSystem;

@Component
public class TestCaseExecutor {
    @Autowired
    private HiveSystem hiveSystem;
    @Autowired
    private PostgreSQLSystem postgreSQLSystem;
    @Autowired
    private MongoDBSystem mongoDBSystem;

    private static final Pattern setPattern = Pattern.compile("(HIVE|SQL|MONGO)\\.SET\\(\\( *([^,]+) *, *([^\\)]+) *\\) *, *([A-F][+-]?) *\\)");
    private static final Pattern getPattern = Pattern.compile("(HIVE|SQL|MONGO)\\.GET\\( *([^,]+) *, *([^\\)]+) *\\)");
    private static final Pattern mergePattern = Pattern.compile("(HIVE|SQL|MONGO)\\.MERGE\\((HIVE|SQL|MONGO)\\)");

    public void executeTestCase() throws Exception {
        ClassPathResource resource = new ClassPathResource("testcase.in");
        InputStream inputStream = resource.getInputStream();
        // BufferedReader reader = new BufferedReader(new FileReader("testcase.in"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;

        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) continue;

            line = line.replaceFirst("^\\d+ *, *", ""); // remove initial timestamp

            Matcher setMatcher = setPattern.matcher(line);
            Matcher getMatcher = getPattern.matcher(line);
            Matcher mergeMatcher = mergePattern.matcher(line);

            if (setMatcher.matches()) {
                String db = setMatcher.group(1).trim();
                String sid = setMatcher.group(2).trim();
                String cid = setMatcher.group(3).trim();
                String grade = setMatcher.group(4).trim();
                handleSet(db, sid, cid, grade);
            } else if (getMatcher.matches()) {
                String db = getMatcher.group(1).trim();
                String sid = getMatcher.group(2).trim();
                String cid = getMatcher.group(3).trim();
                handleGet(db, sid, cid);
            } else if (mergeMatcher.matches()) {
                String targetDb = mergeMatcher.group(1).trim();
                String sourceDb = mergeMatcher.group(2).trim();
                handleMerge(targetDb, sourceDb);
            } else {
                System.out.println("Unrecognized line: " + line);
            }
        }

        reader.close();
    }

    private void handleSet(String db, String sid, String cid, String grade) {
        LocalDateTime ldt = java.time.LocalDateTime.now();
        switch (db.toUpperCase()) {
            case "HIVE":
                hiveSystem.updateGrade(sid, cid, grade, ldt.toString());
                break;
            case "SQL":
                postgreSQLSystem.updateGrade(sid, cid, grade, ldt.toString());
                break;
            case "MONGO":
                mongoDBSystem.updateGrade(sid, cid, grade, ldt.toString());
                break;
        }
    }

    private void handleGet(String db, String sid, String cid) {
        LocalDateTime ldt = java.time.LocalDateTime.now();
        switch (db.toUpperCase()) {
            case "HIVE":
                hiveSystem.readGrade(sid, cid, ldt.toString());
                break;
            case "SQL":
                postgreSQLSystem.readGrade(sid, cid, ldt.toString());
                break;
            case "MONGO":
                mongoDBSystem.readGrade(sid, cid, ldt.toString());
                break;
        }
    }

    private void handleMerge(String targetDb, String sourceDb) {
        System.out.println("Merging from " + sourceDb + " to " + targetDb);
        switch (targetDb.toUpperCase()) {
            case "HIVE":
                hiveSystem.merge(sourceDb);
                break;
            case "SQL":
                postgreSQLSystem.merge(sourceDb);
                break;
            case "MONGO":
                mongoDBSystem.merge(sourceDb);
                break;
        }
    }
}
