package io.tapdata.mongodb;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.it.tpcc.TpccAdapter;
import io.tapdata.it.tpcc.TpccConfig;
import org.bson.Document;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;

final class MongoTpccAdapter implements TpccAdapter {
    private static final List<String> TABLES = Arrays.asList("bmsql_config", "bmsql_warehouse", "bmsql_district",
            "bmsql_customer", "bmsql_history", "bmsql_new_order", "bmsql_oorder", "bmsql_order_line", "bmsql_item", "bmsql_stock");
    private final DataMap config;
    private final AtomicLong sequence = new AtomicLong(1000L);

    MongoTpccAdapter(DataMap config) {
        this.config = config;
    }

    @Override
    public List<String> tableNames() {
        return TABLES;
    }

    @Override
    public boolean isPrepared() {
        try (MongoClient client = client()) {
            for (String table : TABLES) {
                if (collection(client, table).countDocuments() == 0L) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public void prepare(TpccConfig ignored) {
        cleanup();
        try (MongoClient client = client()) {
            database(client).getCollection("bmsql_config").insertOne(new Document("_id", "config").append("cfg_name", "warehouses").append("cfg_value", "1"));
            database(client).getCollection("bmsql_warehouse").insertOne(new Document("_id", "w1").append("w_id", 1).append("w_name", "TapData").append("w_ytd", 0L));
            database(client).getCollection("bmsql_district").insertOne(new Document("_id", "w1-d1").append("d_w_id", 1).append("d_id", 1).append("d_next_o_id", 2L).append("d_ytd", 0L));
            database(client).getCollection("bmsql_customer").insertOne(new Document("_id", "w1-d1-c1").append("c_w_id", 1).append("c_d_id", 1).append("c_id", 1).append("c_balance", 0L));
            database(client).getCollection("bmsql_history").insertOne(new Document("_id", "h1").append("h_c_id", 1).append("h_amount", 0L));
            database(client).getCollection("bmsql_new_order").insertOne(new Document("_id", "w1-d1-o1").append("no_w_id", 1).append("no_d_id", 1).append("no_o_id", 1L));
            database(client).getCollection("bmsql_oorder").insertOne(new Document("_id", "w1-d1-o1").append("o_w_id", 1).append("o_d_id", 1).append("o_id", 1L).append("o_c_id", 1));
            database(client).getCollection("bmsql_order_line").insertOne(new Document("_id", "w1-d1-o1-l1").append("ol_w_id", 1).append("ol_d_id", 1).append("ol_o_id", 1L).append("ol_number", 1).append("ol_i_id", 1));
            database(client).getCollection("bmsql_item").insertOne(new Document("_id", "i1").append("i_id", 1).append("i_name", "Tap item").append("i_price", 100L));
            database(client).getCollection("bmsql_stock").insertOne(new Document("_id", "w1-i1").append("s_w_id", 1).append("s_i_id", 1).append("s_quantity", 100000L));
        }
    }

    @Override
    public Map<String, Long> currentRowCounts() {
        Map<String, Long> counts = new LinkedHashMap<>();
        try (MongoClient client = client()) {
            for (String table : TABLES) {
                counts.put(table, collection(client, table).countDocuments());
            }
        }
        return counts;
    }

    @Override
    public void runWorkload(TpccConfig tpccConfig) {
        int transactions = Math.max(tpccConfig.getMinimumEvents(), Math.max(1, tpccConfig.getTransactionsPerTerminal()));
        try (MongoClient client = client()) {
            for (int index = 0; index < transactions; index++) {
                long orderId = sequence.incrementAndGet();
                try (ClientSession session = client.startSession()) {
                    session.startTransaction();
                    collection(client, "bmsql_district").updateOne(session, eq("_id", "w1-d1"), combine(inc("d_next_o_id", 1L), inc("d_ytd", 1L)));
                    collection(client, "bmsql_customer").updateOne(session, eq("_id", "w1-d1-c1"), inc("c_balance", -1L));
                    collection(client, "bmsql_stock").updateOne(session, eq("_id", "w1-i1"), inc("s_quantity", -1L));
                    collection(client, "bmsql_oorder").insertOne(session, new Document("_id", "w1-d1-o" + orderId).append("o_w_id", 1).append("o_d_id", 1).append("o_id", orderId).append("o_c_id", 1));
                    collection(client, "bmsql_new_order").insertOne(session, new Document("_id", "w1-d1-o" + orderId).append("no_w_id", 1).append("no_d_id", 1).append("no_o_id", orderId));
                    collection(client, "bmsql_order_line").insertOne(session, new Document("_id", "w1-d1-o" + orderId + "-l1").append("ol_w_id", 1).append("ol_d_id", 1).append("ol_o_id", orderId).append("ol_number", 1).append("ol_i_id", 1));
                    collection(client, "bmsql_history").insertOne(session, new Document("_id", "h" + orderId).append("h_c_id", 1).append("h_amount", 1L));
                    session.commitTransaction();
                }
            }
        }
    }

    @Override
    public void verifyConsistency() {
        try (MongoClient client = client()) {
            for (Document orderLine : collection(client, "bmsql_order_line").find()) {
                long orderId = ((Number) orderLine.get("ol_o_id")).longValue();
                if (collection(client, "bmsql_oorder").countDocuments(eq("o_id", orderId)) != 1L) {
                    throw new AssertionError("orphan TPCC order line: " + orderId);
                }
            }
            Document stock = collection(client, "bmsql_stock").find(eq("_id", "w1-i1")).first();
            if (stock == null || ((Number) stock.get("s_quantity")).longValue() < 0L) {
                throw new AssertionError("invalid TPCC stock quantity");
            }
        }
    }

    @Override
    public void cleanup() {
        try (MongoClient client = client()) {
            for (String table : TABLES) {
                collection(client, table).drop();
            }
        }
    }

    private MongoClient client() {
        return MongoClients.create(config.getString("uri"));
    }

    private MongoDatabase database(MongoClient client) {
        return client.getDatabase(config.getString("database"));
    }

    private MongoCollection<Document> collection(MongoClient client, String name) {
        return database(client).getCollection(name);
    }
}
