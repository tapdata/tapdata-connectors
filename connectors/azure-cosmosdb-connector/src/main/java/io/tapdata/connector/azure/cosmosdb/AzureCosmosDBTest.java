package io.tapdata.connector.azure.cosmosdb;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.util.CosmosPagedIterable;
import io.tapdata.common.CommonDbTest;
import io.tapdata.connector.azure.cosmosdb.config.AzureCosmosDBConfig;
import io.tapdata.constant.DbTestItem;
import io.tapdata.pdk.apis.entity.TestItem;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

public class AzureCosmosDBTest extends CommonDbTest {
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
            Iterator<CosmosContainerProperties> iterator = cosmosDatabase.readAllContainers().iterator();
            if(iterator.hasNext()){
                iterator.next();
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
            Iterator<CosmosContainerProperties> iterator = cosmosDatabase.readAllContainers().iterator();
            if (iterator.hasNext()){
                CosmosContainerProperties containerProperties = iterator.next();
                CosmosContainer container = cosmosDatabase.getContainer(containerProperties.getId());
                String sql = "select * from c";
                CosmosPagedIterable<Map> maps = container.queryItems(sql, new CosmosQueryRequestOptions(), Map.class);
                Iterator<Map> page= maps.iterator();
                if (page.hasNext()){
                    page.next();
                }
            }else{
                consumer.accept(testItem(TestItem.ITEM_READ,TestItem.RESULT_SUCCESSFULLY_WITH_WARN,"No Container Found in "+cosmosDBConfig.getDatabaseName()));
            }
            consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
            return true;
        }catch (CosmosException e){
            consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_FAILED, "Missing read privileges on" + cosmosDBConfig.getDatabaseName() + "database"));
            return false;
        }

    }
}
