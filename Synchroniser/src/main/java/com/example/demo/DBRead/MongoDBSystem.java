package com.example.demo.DBRead;
import java.util.ArrayList;

import org.bson.Document;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.MongoDatabase;

public class MongoDBSystem extends DBSystem {
    private MongoCollection<Document> collection;

    public MongoDBSystem() {
        super("mongo");
        MongoClient client = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase db = client.getDatabase("yourdb");
        collection = db.getCollection("grades");
    }

    @Override
    public String readGrade(String studentId,String courseId) {
        Document doc = collection.find(new Document("student_id", studentId)).first();
        if (doc != null) {
            return doc.getString("grade");
        }
        return "Not Found";
    }

    @Override
    public void updateGrade(String studentId,String couseId,String grade) {
        Document doc = new Document("student_id", studentId)
                .append("course_id", couseId)
                .append("grade", grade);
        collection.replaceOne(new Document("student_id", studentId), doc, new ReplaceOptions().upsert(true));
        logOperation("update", studentId, couseId, grade);
    }

    @Override
    public void merge(String fromSystem) {
        for (Operation op : oplogs.getOrDefault(fromSystem, new ArrayList<>())) {
            if (op.opType.equals("update")) {
                updateGrade(op.studentId,op.courseId,op.value);
                logOperation("merge_update", op.studentId, op.courseId,op.value);
            }
        }
    }
}
