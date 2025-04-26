package com.example.demo.DBRead;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOptions;
import com.opencsv.CSVReader;

import jakarta.annotation.PostConstruct;

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

    public MongoDBSystem() throws IOException {
        super("mongo");
    }

    @PostConstruct
    public void initMongo() {
        try {
            String connectionString = String.format("mongodb://%s:%s@localhost:%d", username, password, port);

            mongoClient = MongoClients.create(connectionString);
            database = mongoClient.getDatabase(databaseName);
            collection = database.getCollection(collectionName);

            System.out.println("MongoDB connection initialized.");
        } catch (Exception e) {
            System.out.println("Failed to initialize MongoDB connection: " + getStackTrace(e));
            throw new RuntimeException("Failed to initialize MongoDB connection", e);
        }
    }

    @Override
    public String readGrade(String studentId, String courseId, String timestamp) {
        Document query = new Document("student_id", studentId)
                .append("course_id", courseId);

        Document doc = collection.find(query).first();
        String returnString = (doc == null) ? "Not Found" : doc.getString("grade");
        logAction("read", studentId, courseId, returnString,"mongo", timestamp);

        return returnString;
    }

    @Override
    public void updateGrade(String studentId, String courseId, String grade, String timestamp) {
        Document updatedDoc = new Document("student_id", studentId)
                .append("course_id", courseId)
                .append("grade", grade);

        collection.replaceOne(
                new Document("student_id", studentId).append("course_id", courseId),
                updatedDoc,
                new ReplaceOptions().upsert(false)
        );

        logAction("update", studentId, courseId, grade,"mongo", timestamp);
    }

    @Override
    public void importFile() throws Exception {
        try (CSVReader reader = new CSVReader(new FileReader(csvFilePath))) {
            if (database.listCollectionNames().into(new ArrayList<>()).contains(collectionName)) {
                collection.drop();
                System.out.println("Existing collection dropped.");
            }

            String[] headers = reader.readNext();
            if (headers == null) {
                System.out.println("CSV file is empty.");
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
                System.out.println("Successfully inserted " + documents.size() + " documents.");
            } else {
                System.out.println("No data found in CSV.");
            }

            long count = database.getCollection(collectionName).countDocuments();
            System.out.println("Documents in collection: " + count);
        } catch (Exception e) {
            System.out.println("Error during importFile: " + getStackTrace(e));
        }
    }

    // private void logAction(String action, String studentId, String courseId, String grade) {
    //     String message = String.format("%s - studentId=%s, courseId=%s, grade=%s",
    //             action.toUpperCase(), studentId, courseId, grade);
    //     writeToLogFile(message, "mongo-log.txt");
    // }

    // private void logAction(String action, String studentId, String courseId, String grade, String timeStamp) {
    //     String message = String.format("%s - studentId=%s, courseId=%s, grade=%s",
    //             action.toUpperCase(), studentId, courseId, grade);
    //     writeToLogFile(message, "mongo-log.txt", timeStamp);
    // }

}