package io.tapdata.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.it.performance.PerformanceAdapter;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

final class MongoPerformanceAdapter implements PerformanceAdapter {
    private static final int NUMERIC_FIELDS = 16;
    private static final int STRING_FIELDS = 32;

    private final DataMap config;
    private final String collectionName;
    private final TapTable table;

    MongoPerformanceAdapter(DataMap config) {
        this.config = config;
        this.collectionName = "TAP_MONGO_PERF_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        this.table = createTapTable();
    }

    @Override
    public TapTable table() {
        return table;
    }

    @Override
    public void createTable() {
        try (MongoClient client = client()) {
            database(client).createCollection(collectionName);
        }
    }

    @Override
    public void insertRows(List<Map<String, Object>> rows) {
        if (rows.isEmpty()) {
            return;
        }
        List<Document> documents = new ArrayList<>(rows.size());
        for (Map<String, Object> row : rows) {
            Document document = new Document(row);
            document.put("_id", row.get("ID"));
            Object eventTime = document.get("EVENT_TIME");
            if (eventTime instanceof java.sql.Timestamp) {
                document.put("EVENT_TIME", new Date(((java.sql.Timestamp) eventTime).getTime()));
            }
            documents.add(document);
        }
        try (MongoClient client = client()) {
            collection(client).insertMany(documents);
        }
    }

    @Override
    public long countRows() {
        try (MongoClient client = client()) {
            return collection(client).countDocuments();
        }
    }

    @Override
    public void dropTable() {
        try (MongoClient client = client()) {
            collection(client).drop();
        }
    }

    private TapTable createTapTable() {
        TapTable tapTable = TapSimplify.table(collectionName);
        TapField id = TapSimplify.field("ID", "STRING").tapType(TapSimplify.tapString());
        id.isPrimaryKey(true).primaryKeyPos(1).nullable(false);
        tapTable.add(id);
        tapTable.add(TapSimplify.field("EVENT_TIME", "DATE_TIME").tapType(TapSimplify.tapDateTime()).nullable(false));
        for (int index = 1; index <= NUMERIC_FIELDS; index++) {
            tapTable.add(TapSimplify.field(fieldName("N", index), "LONG").tapType(TapSimplify.tapNumber().bit(64)).nullable(false));
        }
        for (int index = 1; index <= STRING_FIELDS; index++) {
            tapTable.add(TapSimplify.field(fieldName("S", index), "STRING").tapType(TapSimplify.tapString()).nullable(false));
        }
        return tapTable;
    }

    private MongoClient client() {
        return MongoClients.create(config.getString("uri"));
    }

    private MongoDatabase database(MongoClient client) {
        return client.getDatabase(config.getString("database"));
    }

    private MongoCollection<Document> collection(MongoClient client) {
        return database(client).getCollection(collectionName);
    }

    private static String fieldName(String prefix, int index) {
        return String.format("%s%02d", prefix, index);
    }
}
