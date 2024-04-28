package io.tapdata.mongodb;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
public class MD5Test {
    public static void main(String[] args) {
        final String databaseName = "test";
        final String collectionName = "test_md5";
        try (MongoClient mongoClient = MongoClients.create("mongodb://xiao:123456@127.0.0.1:27017")) {
            MongoDatabase admin = mongoClient.getDatabase("admin");
            final MongoDatabase database = mongoClient.getDatabase(databaseName);
            final String functionName = "hash";
            final String jsScript = "function(collectionName, filterJson){" +
                    " return 'hello';" +
                    "}";
            final Document functionDocument = new Document()
                    .append("_id", functionName)
                    .append("value", jsScript);
            MongoCollection<Document> systemJS = database.getCollection("system.js");
            try {
                systemJS.insertOne(functionDocument);
                admin.runCommand(new Document("eval", "load()"));
                Document commandExec = new Document("eval", String.format("hash(%s, %s)", collectionName, "{}"));
                Document resultExec = admin.runCommand(commandExec);
                System.out.println(resultExec.toJson());
            } finally {
                systemJS.deleteOne(new Document().append("_id", functionName));
            }
        }
    }
}
