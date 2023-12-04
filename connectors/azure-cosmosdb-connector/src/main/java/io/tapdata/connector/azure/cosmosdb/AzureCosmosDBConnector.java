package io.tapdata.connector.azure.cosmosdb;

import com.azure.cosmos.*;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.*;
import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.azure.cosmosdb.config.AzureCosmosDBConfig;
import io.tapdata.connector.azure.cosmosdb.util.NumberNodeType;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.TapStringValue;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.*;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.commons.collections4.ListUtils;

import java.io.Closeable;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@TapConnectorClass("azure-cosmosdb-spec.json")
public class AzureCosmosDBConnector extends ConnectorBase {
    public static final String TAG = AzureCosmosDBConnector.class.getSimpleName();
    protected AzureCosmosDBConfig azureCosmosDBConfig;
    protected CosmosClient cosmosClient;
    private CosmosDatabase cosmosDatabase;

    private Map<String, Integer> stringTypeValueMap;
    private static final String COLLECTION_RID_FIELD = "_rid";

    private static final String COLLECTION_ID_FIELD = "id";

    private static final ObjectMapper objectMapper = new ObjectMapper();


    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        //source
        connectorFunctions.supportErrorHandleFunction(this::errorHandle);
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportGetTableNamesFunction(this::getTableNames);
        codecRegistry.registerToTapValue(BigInteger.class, (value, tapType) -> {
            BigInteger objValue = (BigInteger) value;
            return new TapStringValue(objValue.toString());
        });
    }


    private void batchRead(TapConnectorContext connectorContext, TapTable table, Object offset, int eventBatchSize, BiConsumer<List<TapEvent>, Object> tapReadOffsetConsumer) throws Throwable {
        try {
            List<TapEvent> tapEvents = list();
            CosmosContainer cosmosContainer = getCosmosContainer(table.getId());
            final int batchSize = eventBatchSize > 0 ? eventBatchSize : 5000;
            String sql = String.format("select * from c order by c.%s", COLLECTION_ID_FIELD);
//        String sql="select * from c";
            Iterator<FeedResponse<JsonNode>> iterator;
            if (offset == null) {
                iterator = cosmosContainer.queryItems(sql, new CosmosQueryRequestOptions(), JsonNode.class).iterableByPage(null, batchSize).iterator();
            } else {
                CosmosDBBatchOffset cosmosOffset = (CosmosDBBatchOffset) offset;
                String continuationToken = cosmosOffset.getValue();
                if (continuationToken != null) {
                    iterator = cosmosContainer.queryItems(sql, new CosmosQueryRequestOptions(), JsonNode.class).iterableByPage(continuationToken, batchSize).iterator();
                } else {
                    iterator = cosmosContainer.queryItems(sql, new CosmosQueryRequestOptions(), JsonNode.class).iterableByPage(null, batchSize).iterator();
                }
            }
            CosmosDBBatchOffset cosmosDBBatchOffset = null;
            while (iterator.hasNext()) {
                if (!isAlive()) return;
                FeedResponse<JsonNode> next = iterator.next();
                List<JsonNode> results = next.getResults();
                for (JsonNode jsonNode : results) {
                    Map<String, Object> map = objectMapper.convertValue(jsonNode, Map.class);
                    tapEvents.add(insertRecordEvent(map, table.getId()));
                }
                String continuationToken = next.getContinuationToken();
                cosmosDBBatchOffset = new CosmosDBBatchOffset(continuationToken);
                if (!tapEvents.isEmpty()) {
                    tapReadOffsetConsumer.accept(tapEvents, cosmosDBBatchOffset);
                }
                tapEvents = list();
            }
            if (!tapEvents.isEmpty()) {
                tapReadOffsetConsumer.accept(tapEvents, null);
            }
        } catch (Exception e) {
            throw new RuntimeException("Batch Read Failed " + e.getMessage());
        }

    }


    private CosmosContainer getCosmosContainer(String name) {
        CosmosContainer container = cosmosDatabase.getContainer(name);
        return container;
    }

    private long batchCount(TapConnectorContext connectorContext, TapTable table) throws Throwable {
        long count = 0;
        String query = "SELECT  COUNT(1) as count FROM c";
        try {
            CosmosContainer container = cosmosDatabase.getContainer(table.getId());
            CosmosPagedIterable<ObjectNode> resultIterator = container.queryItems(query, new CosmosQueryRequestOptions(), ObjectNode.class);
            for (ObjectNode item : resultIterator) {
                count = item.get("count").longValue();
            }
        } catch (Exception e) {
            throw new RuntimeException("Get Table " + table.getId() + " count Failed! " + e.getMessage());
        }
        return count;
    }


    private void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) throws Throwable {
        String databaseName = azureCosmosDBConfig.getDatabaseName();
        List<String> temp = new ArrayList<>();
        try {
            CosmosPagedIterable<CosmosContainerProperties> cosmosPagedIterable = cosmosClient.getDatabase(databaseName).readAllContainers();
            for (CosmosContainerProperties containerProperties : cosmosPagedIterable) {
                temp.add(containerProperties.getId());
                if (temp.size() >= batchSize) {
                    listConsumer.accept(temp);
                    temp.clear();
                }
            }
            if (!temp.isEmpty()) {
                listConsumer.accept(temp);
                temp.clear();
            }
        } catch (Exception e) {
            throw new RuntimeException("Get TableNames Failed! " + e.getMessage());
        }

    }


    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        try {
            CosmosPagedIterable<CosmosContainerProperties> allContainers = cosmosDatabase.readAllContainers();
            List<CosmosContainerProperties> allContainersProp = allContainers.stream().collect(Collectors.toList());
            TableFieldTypesGenerator tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
            stringTypeValueMap = new HashMap<>();
            ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 30, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(30));
            try (Closeable ignored = executor::shutdown) {
                ListUtils.partition(allContainersProp, tableSize).forEach(containerList -> {
                    CountDownLatch countDownLatch = new CountDownLatch(containerList.size());
                    ConcurrentHashMap<String, CosmosContainer> containerMap = new ConcurrentHashMap<>();
                    containerList.forEach(container -> executor.execute(() -> {
                        String containerName = container.getId();
                        containerMap.put(containerName, cosmosDatabase.getContainer(containerName));
                        countDownLatch.countDown();
                    }));
                    try {
                        countDownLatch.await();
                    } catch (InterruptedException e) {
                        TapLogger.error(TAG, "AzureCosmosDBConnector discoverSchema countDownLatch await", e);
                    }
                    List<TapTable> list = list();
                    containerList.forEach(name -> {
                        TapTable table = new TapTable(name.getId());
                        CosmosContainer container = containerMap.get(name.getId());
                        String sql = "select * from c OFFSET 0 LIMIT 1000";
                        CosmosPagedIterable<JsonNode> resultIterator = container.queryItems(sql, new CosmosQueryRequestOptions(), JsonNode.class);
                        Iterator<JsonNode> iterator = resultIterator.iterator();
                        while (iterator.hasNext()) {
                            JsonNode item = iterator.next();
                            Iterator<Map.Entry<String, JsonNode>> fields = item.fields();
                            while (fields.hasNext()) {
                                Map.Entry<String, JsonNode> field = fields.next();
                                String fieldName = field.getKey();
                                JsonNode fieldValue = field.getValue();
                                getRelateDatabaseField(connectionContext, tableFieldTypesGenerator, fieldValue, fieldName, table);
                            }
                        }
                        if (!Objects.isNull(table.getNameFieldMap()) && !table.getNameFieldMap().isEmpty()) {
                            list.add(table);
                        }
                    });
                    consumer.accept(list);
                });
            }
        } catch (Exception e) {
            throw new RuntimeException("DiscoverSchema failed" + e.getMessage());
        }
    }

    private void getRelateDatabaseField(TapConnectionContext connectionContext, TableFieldTypesGenerator tableFieldTypesGenerator, JsonNode value, String fieldName, TapTable table) {
        if (value instanceof ObjectNode) {
            ObjectNode objectNode = (ObjectNode) value;
            Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                getRelateDatabaseField(connectionContext, tableFieldTypesGenerator, field.getValue(), fieldName + "." + field.getKey(), table);
            }
        } else if (value instanceof ArrayNode) {
            JsonNodeFactory instance = JsonNodeFactory.instance;
            ObjectNode objectNode = instance.objectNode();
            Iterator<Map.Entry<String, JsonNode>> fields = value.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String key = field.getKey();
                JsonNode jsonNodeValue = field.getValue();
                JsonNode jsonNode = objectNode.get(key);
                if ((jsonNode == null || jsonNode.isNull()) || !jsonNodeValue.isNull()) {
                    objectNode.set(key, jsonNodeValue);
                }
            }
            if (!objectNode.isEmpty()) {
                Iterator<Map.Entry<String, JsonNode>> fields1 = objectNode.fields();
                while (fields1.hasNext()) {
                    Map.Entry<String, JsonNode> next = fields1.next();
                    getRelateDatabaseField(connectionContext, tableFieldTypesGenerator, next.getValue(), fieldName + "." + next.getKey(), table);
                }
            }
        }
        TapField tapField = null;
        if (value != null && !value.isNull()) {
            JsonNodeType nodeType = value.getNodeType();
            String nodeTypeName = nodeType.name();
            if (JsonNodeType.STRING.name().equals(nodeTypeName)) {
                int currentLength = ((TextNode) value).textValue().getBytes().length;
                Integer lastLength = stringTypeValueMap.get(fieldName);
                if (currentLength > 0 && (null == lastLength || currentLength > lastLength)) {
                    stringTypeValueMap.put(fieldName, currentLength);
                }
                if (null != stringTypeValueMap.get(fieldName)) {
                    int length = stringTypeValueMap.get(fieldName);
                    length = length * 5;
                    if (length < 100)
                        length = 100;
                    tapField = TapSimplify.field(fieldName, nodeTypeName + String.format("(%s)", length));
                } else {
                    tapField = TapSimplify.field(fieldName, nodeTypeName);
                }
            } else if (JsonNodeType.NUMBER.name().equals(nodeTypeName)) {
                tapField = convertNumberType(value, fieldName);
            } else {
                tapField = TapSimplify.field(fieldName, nodeTypeName);
            }
        } else {
            tapField = TapSimplify.field(fieldName, JsonNodeType.NULL.name());
        }

        TapField currentFiled = null;
        if (table.getNameFieldMap() != null)
            currentFiled = table.getNameFieldMap().get(fieldName);
        if (currentFiled != null &&
                currentFiled.getDataType() != null &&
                !currentFiled.getDataType().equals(JsonNodeType.NULL.name()) &&
                tapField.getDataType() != null && tapField.getDataType().equals(JsonNodeType.NULL.name())
        ) {
            return;
        }

        tableFieldTypesGenerator.autoFill(tapField, connectionContext.getSpecification().getDataTypesMap());
        table.add(tapField);
    }

    private TapField convertNumberType(JsonNode value, String fieldName) {
        TapField tapField = null;
        if (value instanceof FloatNode) {
            tapField = TapSimplify.field(fieldName, NumberNodeType.FLOAT.name());
        } else if (value instanceof DoubleNode) {
            tapField = TapSimplify.field(fieldName, NumberNodeType.DOUBLE.name());
        } else if (value instanceof ShortNode) {
            tapField = TapSimplify.field(fieldName, NumberNodeType.SHORT.name());
        } else if (value instanceof IntNode) {
            tapField = TapSimplify.field(fieldName, NumberNodeType.INT.name());
        } else if (value instanceof LongNode) {
            tapField = TapSimplify.field(fieldName, NumberNodeType.LONG.name());
        } else {
            tapField = TapSimplify.field(fieldName, NumberNodeType.BIGINTEGER.name());
        }
        return tapField;
    }

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        ConnectionOptions connectionOptions = null;
        try {
            onStart(connectionContext);
            ConnectionOptions.create();
            try (AzureCosmosDBTest cosmosDBTest = new AzureCosmosDBTest(azureCosmosDBConfig, consumer, cosmosClient);) {
                cosmosDBTest.testOneByOne();
            }
        } catch (Throwable throwable) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Failed, " + throwable.getMessage()));
        } finally {
            onStop(connectionContext);
        }
        return connectionOptions;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        CosmosPagedIterable<CosmosContainerProperties> containers = null;
        long count = 0;
        try {
            containers = cosmosDatabase.readAllContainers();
            count = containers.stream().count();
        } catch (Exception e) {
            throw new RuntimeException("Get Table Count Failed " + e.getMessage());
        }
        return (int) count;
    }


    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        azureCosmosDBConfig = (AzureCosmosDBConfig) new AzureCosmosDBConfig().load(connectionContext.getConnectionConfig());
        if (azureCosmosDBConfig == null) {
            throw new RuntimeException("load mongo config failed from connection config");
        }
        ConsistencyLevel consistencyLevel = getConsistencyLevel(azureCosmosDBConfig.getConsistencyLevel());
        if (cosmosClient == null) {
            try {
                cosmosClient = new CosmosClientBuilder()
                        .endpoint(azureCosmosDBConfig.getHost())
                        .key(azureCosmosDBConfig.getAccountKey())
                        .consistencyLevel(consistencyLevel)
                        .contentResponseOnWriteEnabled(true)
                        .buildClient();
                cosmosDatabase = cosmosClient.getDatabase(azureCosmosDBConfig.getDatabaseName());
                Iterator<CosmosContainerProperties> iterator = cosmosDatabase.readAllContainers().iterator();
            } catch (Throwable e) {
                throw new RuntimeException(String.format("Create CosmosConnection failed %s", e.getMessage()), e);
            }
        }
    }

    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        try {
            if (cosmosClient != null) {
                cosmosClient.close();
                cosmosClient = null;
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Close CosmosClient failed %s", e.getMessage()), e);
        }
    }

    protected ConsistencyLevel getConsistencyLevel(String consistencyLevel) {
        switch (consistencyLevel) {
            case "Strong":
                return ConsistencyLevel.STRONG;
            case "BoundedStaleness":
                return ConsistencyLevel.BOUNDED_STALENESS;
            case "Session":
                return ConsistencyLevel.SESSION;
            case "Eventual":
                return ConsistencyLevel.EVENTUAL;
            case "ConsistentPrefix":
                return ConsistencyLevel.CONSISTENT_PREFIX;
            default:
                return null;
        }
    }

}
