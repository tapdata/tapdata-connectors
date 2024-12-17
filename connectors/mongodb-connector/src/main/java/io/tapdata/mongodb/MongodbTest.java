package io.tapdata.mongodb;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.tapdata.common.CommonDbTest;
import io.tapdata.constant.DbTestItem;
import io.tapdata.exception.TapCodeException;
import io.tapdata.kit.EmptyKit;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.mongodb.error.MongodbErrorCode;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.exception.TapTestItemException;
import io.tapdata.pdk.apis.exception.testItem.TapTestConnectionEx;
import io.tapdata.pdk.apis.exception.testItem.TapTestCurrentTimeConsistentEx;
import io.tapdata.pdk.apis.exception.testItem.TapTestVersionEx;
import io.tapdata.util.NetUtil;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;

import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static io.tapdata.base.ConnectorBase.testItem;

public class MongodbTest extends CommonDbTest {

    protected final MongodbConfig mongodbConfig;
    protected final MongoClient mongoClient;

    protected final ConnectionOptions connectionOptions;

    protected static final Set<String> READ_PRIVILEGE_ACTIONS = new HashSet<>();
    private static final Set<String> READ_WRITE_PRIVILEGE_ACTIONS = new HashSet<>();

    private static final String CONFIG_DATABASE_SHARDS_COLLECTION = "config.shards";
    private static final String CONFIG_DATABASE_COLLECTIONS = "config.collections";
    private static final String LOCAL_DATABASEOPLOG_COLLECTION = "local.oplog.rs";
    private static final String CONFIG_DATABASE = "config";
    private static final String LOCAL_DATABASE = "local";
    private static final String ADMIN_DATABASE = "admin";
    private final MongodbExceptionCollector exceptionCollector;

    static {

        READ_PRIVILEGE_ACTIONS.add("collStats");
        READ_PRIVILEGE_ACTIONS.add("dbStats");
        READ_PRIVILEGE_ACTIONS.add("find");
        READ_PRIVILEGE_ACTIONS.add("killCursors");
        READ_PRIVILEGE_ACTIONS.add("listCollections");
        READ_PRIVILEGE_ACTIONS.add("listIndexes");

        READ_WRITE_PRIVILEGE_ACTIONS.add("collStats");
        READ_WRITE_PRIVILEGE_ACTIONS.add("dbStats");
        READ_WRITE_PRIVILEGE_ACTIONS.add("createCollection");
        READ_WRITE_PRIVILEGE_ACTIONS.add("createIndex");
        READ_WRITE_PRIVILEGE_ACTIONS.add("dropCollection");
        READ_WRITE_PRIVILEGE_ACTIONS.add("dropIndex");
        READ_WRITE_PRIVILEGE_ACTIONS.add("find");
        READ_WRITE_PRIVILEGE_ACTIONS.add("insert");
        READ_WRITE_PRIVILEGE_ACTIONS.add("killCursors");
        READ_WRITE_PRIVILEGE_ACTIONS.add("listCollections");
        READ_WRITE_PRIVILEGE_ACTIONS.add("listIndexes");
        READ_WRITE_PRIVILEGE_ACTIONS.add("remove");
        READ_WRITE_PRIVILEGE_ACTIONS.add("renameCollectionSameDB");
        READ_WRITE_PRIVILEGE_ACTIONS.add("update");
    }

    public MongodbTest(MongodbConfig mongodbConfig, Consumer<TestItem> consumer, MongoClient mongoClient, ConnectionOptions connectionOptions) {
        super(mongodbConfig, consumer);
        this.mongodbConfig = mongodbConfig;
        this.mongoClient = mongoClient;
        this.connectionOptions = connectionOptions;
        this.exceptionCollector = new MongodbExceptionCollector();
    }

    @Override
    protected List<String> supportVersions() {
        return Collections.singletonList("*.*");
    }

    @Override
    public Boolean testHostPort() {
        StringBuilder failHosts;
        List<String> hosts = mongodbConfig.getHosts();
        failHosts = new StringBuilder();
        for (String host : hosts) {
            String[] split = host.split(":");
            String hostname = split[0];
            int port = 27017;
            if (split.length > 1) {
                port = Integer.parseInt(split[1]);
            }
            try {
                NetUtil.validateHostPortWithSocket(hostname, port);
            } catch (Exception e) {
                failHosts.append(host).append(",");
            }
        }
        if (EmptyKit.isNotBlank(String.valueOf(failHosts))) {
            failHosts = new StringBuilder(failHosts.substring(0, failHosts.length() - 1));
            consumer.accept(testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED, JSONObject.toJSONString(failHosts)));
            return false;
        }
        consumer.accept(testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY));
        return true;
    }

    @Override
    public Boolean testConnect() {
        try {
            MongoDatabase mongoDatabase = mongoClient.getDatabase(mongodbConfig.getDatabase());
            try (final MongoCursor<String> mongoCursor = mongoDatabase.listCollectionNames().iterator()) {
                mongoCursor.hasNext();
            }
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY, TEST_CONNECTION_LOGIN));
            return true;
        } catch (Throwable e) {
            try {
                exceptionCollector.collectUserPwdInvalid(mongodbConfig.getUri(), e);
            } catch (TapCodeException ex) {
                consumer.accept(new TestItem(TestItem.ITEM_CONNECTION, new TapTestItemException(ex), TestItem.RESULT_FAILED));
                return false;
            }
            consumer.accept(new TestItem(TestItem.ITEM_CONNECTION, new TapTestConnectionEx(e), TestItem.RESULT_FAILED));
            return false;
        }
    }

    @Override
    public Boolean testVersion() {
        try {
            String version = MongodbUtil.getVersionString(mongoClient, mongodbConfig.getDatabase());
            String versionMsg = "MongoDB version: " + version;
            connectionOptions.setDbVersion(version);
            if (supportVersions().stream().noneMatch(v -> {
                String reg = v.replaceAll("\\*", ".*");
                Pattern pattern = Pattern.compile(reg);
                return pattern.matcher(version).matches();
            })) {
                consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, versionMsg + " is not officially supported. Please report the issues to us if you encounter any."));
            } else {
                consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY, versionMsg));
            }
        } catch (Exception e) {
            consumer.accept(new TestItem(TestItem.ITEM_VERSION, new TapTestVersionEx(e), TestItem.RESULT_FAILED));
        }
        return true;
    }

    @Override
    public Boolean testReadPrivilege() {
        if (!isOpenAuth()) {
            consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
            return true;
        }
        String database = mongodbConfig.getDatabase();
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        Document connectionStatus = mongoDatabase.runCommand(new Document("connectionStatus", 1).append("showPrivileges", 1));
        Document isMaster = mongoDatabase.runCommand(new Document("isMaster", 1));

        if (isMaster.containsKey("msg") && "isdbgrid".equals(isMaster.getString("msg"))) {
            // validate mongos config.shards and source DB privileges
            return validateMongodb(connectionStatus);
        } else if (isMaster.containsKey("setName")) {   // validate replica set
//            if (!validateAuthDB(connectionStatus)) {
//                return false;
//            }
            if (!validateReadOrWriteDatabase(connectionStatus, database, READ_PRIVILEGE_ACTIONS)) {
                consumer.accept(new TestItem(TestItem.ITEM_READ, new TapTestItemException(new TapCodeException(MongodbErrorCode.READ_PRIVILEGES_MISSING, "Missing read privileges")), TestItem.RESULT_FAILED));
                return false;
            }
            if (!validateOplog(connectionStatus)) {
                return false;
            }

            consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
            return true;
        } else {
            consumer.accept(new TestItem(TestItem.ITEM_READ, new TapTestItemException(new TapCodeException(MongodbErrorCode.NO_REPLICA_SET, "Source mongodb instance must be the shards or replica set.")), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
            return false;
        }
    }

    @Override
    public Boolean testWritePrivilege() {
        if (!isOpenAuth()) {
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
            return true;
        }
        MongoDatabase mongoDatabase = mongoClient.getDatabase(mongodbConfig.getDatabase());
        Document connectionStatus = mongoDatabase.runCommand(new Document("connectionStatus", 1).append("showPrivileges", 1));
        if (!validateReadOrWriteDatabase(connectionStatus, mongodbConfig.getDatabase(), READ_WRITE_PRIVILEGE_ACTIONS)) {
            consumer.accept(new TestItem(TestItem.ITEM_WRITE, new TapTestItemException(new TapCodeException(MongodbErrorCode.WRITE_PRIVILEGES_MISSING, "Missing write privileges")), TestItem.RESULT_FAILED));
            return false;
        }
        Document isMaster = mongoDatabase.runCommand(new Document("isMaster", 1));
        if (!isMaster.containsKey("msg") && !"isdbgrid".equals(isMaster.getString("msg")) && !isMaster.containsKey("setName")) {
            consumer.accept(new TestItem(TestItem.ITEM_WRITE, new TapTestItemException(new TapCodeException(MongodbErrorCode.NO_REPLICA_SET, "Not replicaSet or shards")), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
            return true;
        } else if (isMaster.containsKey("msg") && "isdbgrid".equals(isMaster.getString("msg"))) {
            return validateMongodbShardKeys(connectionStatus);
        }
        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
        return true;
    }


    protected boolean validateReadOrWriteDatabase(Document connectionStatus, String database, Set<String> expectActions) {
        Document nodeAuthInfo = connectionStatus.get("authInfo", Document.class);
        List authUserPrivileges = nodeAuthInfo.get("authenticatedUserPrivileges", List.class);

        if (CollectionUtils.isEmpty(authUserPrivileges)) {
            return false;
        }
        Map<String, Set<String>> resourcePrivilegesMap = adaptResourcePrivilegesMap(authUserPrivileges);
        if (!resourcePrivilegesMap.containsKey(database) && !resourcePrivilegesMap.containsKey("")) {
            return false;
        }
        Set<String> sourceDBPrivilegeSet = resourcePrivilegesMap.get(database);
        if (sourceDBPrivilegeSet == null) {
            sourceDBPrivilegeSet = resourcePrivilegesMap.get("");
        } else {
            Set<String> allDatabasePrivilegeSet = resourcePrivilegesMap.get("");
            if (null != allDatabasePrivilegeSet) {
                sourceDBPrivilegeSet.addAll(allDatabasePrivilegeSet);
            }
        }
        if (sourceDBPrivilegeSet == null || !sourceDBPrivilegeSet.containsAll(expectActions)) {
            return false;
        }
        return true;
    }


    protected Map<String, Set<String>> adaptResourcePrivilegesMap(List<Document> authUserPrivileges) {
        Map<String, Set<String>> resourcePrivilegesMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(authUserPrivileges)) {
            for (Document authUserPrivilege : authUserPrivileges) {
                Document resource = authUserPrivilege.get("resource", Document.class);
                List actions = authUserPrivilege.get("actions", List.class);
                String db = resource.getString("db");
                String collection = resource.getString("collection");
                StringBuilder sb = new StringBuilder();
                if (EmptyKit.isNotBlank(db)) {
                    sb.append(db);
                    if (EmptyKit.isNotBlank(collection)) {
                        sb.append(".").append(collection);
                    }
                }
                if (!resourcePrivilegesMap.containsKey(sb.toString())) {
                    resourcePrivilegesMap.put(sb.toString(), new HashSet<>());
                }
                resourcePrivilegesMap.get(sb.toString()).addAll(actions);
            }
        }
        return resourcePrivilegesMap;
    }

    protected boolean isOpenAuth() {
        String databaseUri = mongodbConfig.getUri();
        String username = mongodbConfig.getUser();
        String password = mongodbConfig.getPassword();
        if (EmptyKit.isNotBlank(databaseUri)) {
            ConnectionString connectionString = new ConnectionString(databaseUri);
            username = connectionString.getUsername();
            char[] passwordChars = connectionString.getPassword();
            if (passwordChars != null && passwordChars.length > 0) {
                password = new String(passwordChars);
            }

        }
        if (EmptyKit.isBlank(username) || EmptyKit.isBlank(password)) {
            return false;
        }
        return true;
    }

    protected Boolean validateMongodbShardKeys(Document connectionStatus) {
        Document nodeAuthInfo = connectionStatus.get("authInfo", Document.class);
        List authUserPrivileges = nodeAuthInfo.get("authenticatedUserPrivileges", List.class);
        Map<String, Set<String>> resourcePrivilegesMap = adaptResourcePrivilegesMap(authUserPrivileges);
        if (!resourcePrivilegesMap.containsKey(CONFIG_DATABASE) && !resourcePrivilegesMap.containsKey(CONFIG_DATABASE_COLLECTIONS)) {
            consumer.accept(new TestItem(TestItem.ITEM_WRITE, new TapTestItemException(new TapCodeException(MongodbErrorCode.READ_CONFIG_DB_FAIL, "Missing config read privileges")), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
            return true;
        }
        Set<String> configDBPrivilegeSet = resourcePrivilegesMap.get(CONFIG_DATABASE);
        if (configDBPrivilegeSet == null) {
            configDBPrivilegeSet = resourcePrivilegesMap.get(CONFIG_DATABASE_COLLECTIONS);
        }
        if (configDBPrivilegeSet == null || !configDBPrivilegeSet.containsAll(READ_PRIVILEGE_ACTIONS)) {
            consumer.accept(new TestItem(TestItem.ITEM_WRITE, new TapTestItemException(new TapCodeException(MongodbErrorCode.READ_CONFIG_DB_FAIL, "Missing config.shards read privileges")), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
            return true;
        }
        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
        return true;
    }


    protected boolean validateMongodb(Document connectionStatus) {
        Document nodeAuthInfo = connectionStatus.get("authInfo", Document.class);
        List authUserPrivileges = nodeAuthInfo.get("authenticatedUserPrivileges", List.class);

        Map<String, Set<String>> resourcePrivilegesMap = adaptResourcePrivilegesMap(authUserPrivileges);
        if (!resourcePrivilegesMap.containsKey(CONFIG_DATABASE) && !resourcePrivilegesMap.containsKey(CONFIG_DATABASE_SHARDS_COLLECTION)) {
            consumer.accept(new TestItem(TestItem.ITEM_READ, new TapTestItemException(new TapCodeException(MongodbErrorCode.READ_CONFIG_DB_FAIL, "Missing config.shards read privileges")), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
            return false;
        }
        Set<String> configDBPrivilegeSet = resourcePrivilegesMap.get(CONFIG_DATABASE);
        if (configDBPrivilegeSet == null) {
            configDBPrivilegeSet = resourcePrivilegesMap.get(CONFIG_DATABASE_SHARDS_COLLECTION);
        }

        if (configDBPrivilegeSet == null || !configDBPrivilegeSet.containsAll(READ_PRIVILEGE_ACTIONS)) {
            Set<String> missActions = new HashSet<>(READ_PRIVILEGE_ACTIONS);
            missActions.removeAll(configDBPrivilegeSet);
            consumer.accept(new TestItem(TestItem.ITEM_READ, new TapTestItemException(new TapCodeException(MongodbErrorCode.READ_CONFIG_DB_FAIL, "Missing config.shards read privileges")), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
            return false;
        }
        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
        return true;
    }


    protected boolean validateOplog(Document connectionStatus) {
        Document nodeAuthInfo = connectionStatus.get("authInfo", Document.class);
        List authUserPrivileges = nodeAuthInfo.get("authenticatedUserPrivileges", List.class);
        if (CollectionUtils.isNotEmpty(authUserPrivileges)) {
            Map<String, Set<String>> resourcePrivilegesMap = adaptResourcePrivilegesMap(authUserPrivileges);
            if (!resourcePrivilegesMap.containsKey(LOCAL_DATABASE) && !resourcePrivilegesMap.containsKey(LOCAL_DATABASEOPLOG_COLLECTION)) {
                consumer.accept(new TestItem(TestItem.ITEM_READ, new TapTestItemException(new TapCodeException(MongodbErrorCode.READ_LOCAL_DB_FAIL, "Missing local.oplog.rs read privileges")), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
                return false;
            }
        }
        return true;
    }


    protected boolean validateAuthDB(Document connectionStatus) {
        Document nodeAuthInfo = connectionStatus.get("authInfo", Document.class);
        List nodeAuthenticatedUsers = nodeAuthInfo.get("authenticatedUsers", List.class);
        if (CollectionUtils.isNotEmpty(nodeAuthenticatedUsers)) {
            Document nodeAuthUser = (Document) nodeAuthenticatedUsers.get(0);
            String nodeAuthDB = nodeAuthUser.getString("db");
            if (!ADMIN_DATABASE.equals(nodeAuthDB)) {
                consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED,
                        "Authentication database is not admin, will not be able to use the incremental sync feature."));
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean testStreamRead() {
        Map<String, String> nodeConnURIs = MongodbUtil.nodesURI(mongoClient, mongodbConfig.getUri());
        if (nodeConnURIs.size() == 0 || nodeConnURIs.get("single") != null) {
            consumer.accept(new TestItem(TestItem.ITEM_READ_LOG, new TapTestItemException(new TapCodeException(MongodbErrorCode.NO_REPLICA_SET, "mongodb standalone mode not support cdc.")), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
            return false;
        }
        consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY));
        return true;
    }

    public Boolean testTimeDifference() {
        try {
            Long nowTime = MongodbUtil.getServerTime(mongoClient, mongodbConfig.getDatabase());
            connectionOptions.setTimeDifference(getTimeDifference(nowTime));
        } catch (MongoException e) {
            consumer.accept(new TestItem(TestItem.ITEM_TIME_DETECTION, new TapTestCurrentTimeConsistentEx(e), TestItem.RESULT_SUCCESSFULLY_WITH_WARN));
        }
        return true;
    }


}
