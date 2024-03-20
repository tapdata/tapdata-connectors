package io.tapdata.mongodb.atlasTest;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import io.tapdata.mongodb.MongodbTest;
import io.tapdata.mongodb.MongodbUtil;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

/**
 * Author:Skeet
 * Date: 2023/3/27
 **/
public class MongodbAtlasTest extends MongodbTest {

    protected static final Set<String> ATLAS_READ_PRIVILEGE_ACTIONS = new HashSet<>();

    static {
        ATLAS_READ_PRIVILEGE_ACTIONS.add("find");
        ATLAS_READ_PRIVILEGE_ACTIONS.add("listIndexes");
    }
    public MongodbAtlasTest(MongodbConfig mongodbConfig, Consumer<TestItem> consumer, MongoClient mongoClient, ConnectionOptions connectionOptions) {
        super(mongodbConfig, consumer, mongoClient,connectionOptions);
        testFunctionMap.remove("testHostPort");
    }

    @Override
    public Boolean testConnect() {
        try {
            MongoDatabase mongoDatabase = mongoClient.getDatabase(mongodbConfig.getDatabase());
            mongoDatabase.runCommand(new Document("listCollections",1).append("authorizedCollections",true).append("nameOnly",true)).get("cursor");
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY, TEST_CONNECTION_LOGIN));
            return true;
        } catch (Throwable e) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
    }



    @Override
    public Boolean testStreamRead() {
        try {
            Map<String, String> nodeConnURIs = MongodbUtil.nodesURI(mongoClient, mongodbConfig.getUri());
            if (nodeConnURIs.size() == 0 || nodeConnURIs.get("single") != null) {
                consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "mongodb standalone mode not support cdc."));
                return false;
            }
            consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY));
            return true;
        }catch (Exception e){
            consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, e.getMessage()));
            return false;
        }

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
            if (!validateAuthDB(connectionStatus)) {
                return false;
            }
            if (!validateReadOrWriteDatabase(connectionStatus, database, ATLAS_READ_PRIVILEGE_ACTIONS)) {
                consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_FAILED, "Missing read privileges on" + mongodbConfig.getDatabase() + "database"));
                return false;
            }
            consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
            return true;
        } else {
            consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "Source mongodb instance must be the shards or replica set."));
            return false;
        }
    }

    protected boolean validateReadOrWriteDatabase(Document connectionStatus, String database, Set<String> expectActions) {
        Document nodeAuthInfo = connectionStatus.get("authInfo", Document.class);
        List authUserPrivileges = nodeAuthInfo.get("authenticatedUserPrivileges", List.class);

        if (CollectionUtils.isEmpty(authUserPrivileges)) {
            return false;
        }
        Map<String, Set<String>> resourcePrivilegesMap = adaptResourcePrivilegesMap(authUserPrivileges);
        for (Map.Entry<String, Set<String>>  entry :resourcePrivilegesMap.entrySet()) {
            Set<String> sourceDBPrivilegeSet = entry.getValue();
            if((entry.getKey().contains(database) || entry.getKey().equals("")) && entry.getValue() != null && sourceDBPrivilegeSet.containsAll(expectActions)) {
                return true;
            }
        }
        return false;
    }


}