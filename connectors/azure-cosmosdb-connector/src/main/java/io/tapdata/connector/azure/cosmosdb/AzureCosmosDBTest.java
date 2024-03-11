package io.tapdata.connector.azure.cosmosdb;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.tapdata.common.CommonDbTest;
import io.tapdata.connector.azure.cosmosdb.config.AzureCosmosDBConfig;
import io.tapdata.constant.DbTestItem;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

public class AzureCosmosDBTest extends CommonDbTest {
    public static final String TAG = AzureCosmosDBConnector.class.getSimpleName();
    protected final AzureCosmosDBConfig cosmosDBConfig;

    protected final CosmosClient cosmosClient;

    public AzureCosmosDBTest(AzureCosmosDBConfig cosmosDBConfig, Consumer<TestItem> consumer, CosmosClient cosmosClient) {
        super(cosmosDBConfig, consumer);
        this.cosmosDBConfig= cosmosDBConfig;
        this.cosmosClient = cosmosClient;
    }

    @Override
    protected List<String> supportVersions() {
        return Arrays.asList("4.52.*");
    }

    @Override
    protected Boolean testHostPort() {
        consumer.accept(testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY));
        return true;
    }

    @Override
    protected Boolean testConnect() {
        try {
            CosmosDatabase cosmosDatabase = cosmosClient.getDatabase(cosmosDBConfig.getDatabaseName());
            Iterator<CosmosContainerProperties> databaseIterator = cosmosDatabase.readAllContainers().iterator();
            if (databaseIterator.hasNext()) {
                databaseIterator.next();
            }
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY, TEST_CONNECTION_LOGIN));
            return true;
        } catch (Throwable e) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }
    }

    @Override
    protected Boolean testVersion() {
        consumer.accept(testItem(TestItem.ITEM_VERSION, TestItem.RESULT_SUCCESSFULLY, "Java SDK version 4.52.0"));
        return true;
    }


    @Override
    public Boolean testReadPrivilege() {
        CosmosDatabase cosmosDatabase = cosmosClient.getDatabase(cosmosDBConfig.getDatabaseName());
        try{
            Iterator<CosmosContainerProperties> containerIterator = cosmosDatabase.readAllContainers().iterator();
            CosmosContainerProperties containerProperties = null;
            if (containerIterator.hasNext()) {
                containerProperties = containerIterator.next();
                CosmosContainer container = cosmosDatabase.getContainer(containerProperties.getId());
                String query = "SELECT  COUNT(1) as count FROM c";
                Iterator<ObjectNode> iterator = container.queryItems(query, new CosmosQueryRequestOptions(), ObjectNode.class).iterator();
                if(iterator.hasNext()){
                    ObjectNode objectNode = iterator.next();
                }
            }
            consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
            return true;
        }catch (CosmosException e){
            consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_FAILED, "Missing read privileges on" + cosmosDBConfig.getDatabaseName() + " database reson: "+e.getMessage()));
            return false;
        }

    }
}
