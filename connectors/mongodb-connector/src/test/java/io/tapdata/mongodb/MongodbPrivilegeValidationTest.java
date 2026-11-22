package io.tapdata.mongodb;

import com.mongodb.client.MongoClient;
import io.tapdata.mongodb.entity.MongodbConfig;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MongodbPrivilegeValidationTest {
    private final MongodbConfig config = new MongodbConfig();
    private final MongodbTest subject = new MongodbTest(config, item -> {}, mock(MongoClient.class), null);

    @Test
    void databaseAndGlobalPrivilegesCombineWithoutGrantingOtherDatabases() {
        Set<String> required = new HashSet<>(Arrays.asList("find", "listCollections"));
        Document granted = status(privilege("orders", "", "find"), privilege("", "", "listCollections"));
        assertTrue(subject.validateReadOrWriteDatabase(granted, "orders", required));
        assertFalse(subject.validateReadOrWriteDatabase(granted, "customers", required));
        assertFalse(subject.validateReadOrWriteDatabase(status(), "orders", required));
        assertFalse(subject.validateReadOrWriteDatabase(status(privilege("other", "", "find")), "orders", required));
        assertTrue(subject.validateReadOrWriteDatabase(status(privilege("", "", "find", "listCollections")), "orders", required));
    }

    @Test
    void oplogPrivilegesAcceptDatabaseOrExactCollection() {
        assertTrue(subject.validateOplog(status(privilege("local", "", "find"))));
        assertTrue(subject.validateOplog(status(privilege("local", "oplog.rs", "find"))));
        assertFalse(subject.validateOplog(status(privilege("orders", "", "find"))));
        assertTrue(subject.validateOplog(status()));
    }

    @Test
    void shardedSourceRequiresConfigReadPrivileges() {
        assertFalse(subject.validateMongodb(status(privilege("orders", "", "find"))));
        assertFalse(subject.validateMongodb(status(privilege("config", "shards", "find"))));
        assertTrue(subject.validateMongodb(status(privilege("config", "shards", readActions()))));
        assertTrue(subject.validateMongodb(status(privilege("config", "", readActions()))));
    }

    @Test
    void shardingWriteProbeRemainsAnOptionalWarning() {
        assertTrue(subject.validateMongodbShardKeys(status()));
        assertTrue(subject.validateMongodbShardKeys(status(privilege("config", "collections", "find"))));
        assertTrue(subject.validateMongodbShardKeys(status(privilege("config", "collections", readActions()))));
    }

    @Test
    void authenticationRequiresBothUserAndPasswordAndHonorsUriCredentials() {
        config.setUser("reader");
        assertFalse(subject.isOpenAuth());
        config.setPassword("test-only");
        assertTrue(subject.isOpenAuth());
        config.setUser("");
        assertFalse(subject.isOpenAuth());
        config.setUri("mongodb://reader:test-only@localhost/orders");
        assertTrue(subject.isOpenAuth());
        config.setUri("mongodb://localhost/orders");
        assertFalse(subject.isOpenAuth());
    }

    private static String[] readActions() {
        return new String[]{"collStats", "dbStats", "find", "killCursors", "listCollections", "listIndexes"};
    }

    private static Document privilege(String database, String collection, String... actions) {
        return new Document("resource", new Document("db", database).append("collection", collection))
                .append("actions", Arrays.asList(actions));
    }

    private static Document status(Document... privileges) {
        return new Document("authInfo", new Document("authenticatedUserPrivileges", Arrays.asList(privileges)));
    }
}
