package com.example.demo.DBRead;

import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

import jakarta.annotation.PostConstruct;
import org.bson.Document;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import com.opencsv.CSVReader;
import org.springframework.beans.factory.annotation.Value;

@Configuration
@PropertySource("classpath:application.properties")
public class MongoDBSystem extends DBSystem {

    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    @Value("${mongo.port}")
    private Integer port;

    @Value("${mongo.username}")
    private String username;

    @Value("${mongo.password}")
    private String password;

    @Value("${csv.file.path}")
    private String csvFilePath;

    private final String databaseName = "student_course_grades";
    private final String collectionName = "grades";

    public MongoDBSystem() {
        super("mongo");
    }

    @PostConstruct
    public void initMongo() {
        try {
            String connectionString = String.format("mongodb://%s:%s@localhost:%d", username, password, port);

            mongoClient = MongoClients.create(connectionString);
            database = mongoClient.getDatabase(databaseName);
            collection = database.getCollection(collectionName);

            writeToLogFile("MongoDB connection initialized.");
        } catch (Exception e) {
            writeToLogFile("Failed to initialize MongoDB connection: " + getStackTrace(e));
            throw new RuntimeException("Failed to initialize MongoDB connection", e);
        }
    }

    @Override
    public String readGrade(String studentId, String courseId) {
        Document query = new Document("student_id", studentId)
                .append("course_id", courseId);

        Document doc = collection.find(query).first();
        String returnString = (doc == null) ? "Not Found" : doc.getString("grade");

        logOperation("read", studentId, courseId, returnString);
        logAction("read", studentId, courseId, returnString);

        return returnString;
    }

    @Override
    public void updateGrade(String studentId, String courseId, String grade) {
        Document updatedDoc = new Document("student_id", studentId)
                .append("course_id", courseId)
                .append("grade", grade);

        collection.replaceOne(
                new Document("student_id", studentId).append("course_id", courseId),
                updatedDoc,
                new ReplaceOptions().upsert(false)
        );

        logOperation("update", studentId, courseId, grade);
        logAction("update", studentId, courseId, grade);
    }

    @Override
    public void merge(String fromSystem) {
        for (Operation op : oplogs.getOrDefault(fromSystem, new ArrayList<>())) {
            if (op.opType.equals("update")) {
                updateGrade(op.studentId, op.courseId, op.value);
                logOperation("merge_update", op.studentId, op.courseId, op.value);
                logAction("merge_update", op.studentId, op.courseId, op.value);
            }
        }
    }

    public void importFile() {
        try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
            if (database.listCollectionNames().into(new ArrayList<>()).contains(collectionName)) {
                collection.drop();
                writeToLogFile("Existing collection dropped.");
            }

            String[] headers = reader.readNext();
            if (headers == null) {
                writeToLogFile("CSV file is empty.");
                return;
            }

            headers = new String[]{"student_id", "course_id", "roll_no", "email_id", "grade"};

            String[] line;
            List<Document> documents = new ArrayList<>();

            while ((line = reader.readNext()) != null) {
                Document doc = new Document();
                for (int i = 0; i < headers.length; i++) {
                    String key = headers[i].trim().replace(" ", "_");
                    doc.append(key, line[i]);
                }
                documents.add(doc);
            }

            if (!documents.isEmpty()) {
                collection.insertMany(documents);
                writeToLogFile("Successfully inserted " + documents.size() + " documents.");
            } else {
                writeToLogFile("No data found in CSV.");
            }

            long count = database.getCollection(collectionName).countDocuments();
            writeToLogFile("Documents in collection: " + count);
        } catch (Exception e) {
            writeToLogFile("Error during importFile: " + getStackTrace(e));
        }
    }

    private void logAction(String action, String studentId, String courseId, String grade) {
        String message = String.format("%s - studentId=%s, courseId=%s, grade=%s",
                action.toUpperCase(), studentId, courseId, grade);
        writeToLogFile(message);
    }

    private void writeToLogFile(String message) {
        try {
            Path logPath = Paths.get("src/main/resources/mongo-log.txt");
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

    private String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }
}