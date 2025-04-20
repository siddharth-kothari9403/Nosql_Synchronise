package com.example.demo.DBRead;
import java.util.ArrayList;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

class MongoDBSystem extends DBSystem {
    private MongoCollection<Document> collection;

    public MongoDBSystem() {
        super("mongo");
        MongoClient client = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase db = client.getDatabase("yourdb");
        collection = db.getCollection("grades");
    }

    @Override
    public String readGrade(String studentId) {
        Document doc = collection.find(new Document("student_id", studentId)).first();
        return doc != null ? doc.getString("grade") : "Not Found";
    }

    @Override
    public void updateGrade(String studentId, String grade) {
        collection.updateOne(
            new Document("student_id", studentId),
            new Document("$set", new Document("grade", grade)),
            new com.mongodb.client.model.UpdateOptions().upsert(true)
        );
        logOperation("update", studentId, grade);
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
