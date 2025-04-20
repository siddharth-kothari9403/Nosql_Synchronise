package com.example.demo.ImportCSVs;
import com.mongodb.client.*;
import com.opencsv.CSVReader;
import org.bson.Document;

import java.io.FileReader;
import java.util.*;

public class CsvToMongoImporter {

    public static void importFile() {
        String csvFilePath = "./data/student_course_grades.csv"; // Replace with your CSV path
        String mongoUri = "mongodb://localhost:27017";
        String databaseName = "student_course_grades";
        String collectionName = "student_grades";

        try (
                MongoClient mongoClient = MongoClients.create(mongoUri);
                CSVReader reader = new CSVReader(new FileReader(csvFilePath))
        ) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            if (database.listCollectionNames().into(new ArrayList<>()).contains(collectionName)) {
                collection.drop();
                System.out.println("Existing collection dropped.");
            }

            String[] headers = reader.readNext(); // First row as header
            if (headers == null) {
                System.out.println("CSV file is empty.");
                return;
            }

            String[] line;
            List<Document> documents = new ArrayList<>();

            while ((line = reader.readNext()) != null) {
                Document doc = new Document();
                for (int i = 0; i < headers.length; i++) {
                    String key = headers[i].trim().replace(" ", "_"); // MongoDB doesn't support spaces in keys
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

            for (String dbName : mongoClient.listDatabaseNames()) {
                System.out.println("Database: " + dbName);
            }

            database = mongoClient.getDatabase(databaseName);
            for (String collName : database.listCollectionNames()) {
                System.out.println("Collection: " + collName);
            }

            long count = database.getCollection(collectionName).countDocuments();
            System.out.println("Documents in student_grades: " + count);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
