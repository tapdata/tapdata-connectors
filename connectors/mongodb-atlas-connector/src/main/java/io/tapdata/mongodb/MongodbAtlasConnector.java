package io.tapdata.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.mongodb.atlasTest.MongodbAtlasTest;
import io.tapdata.mongodb.util.MongoShardUtil;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TestItem;
import org.apache.commons.collections4.ListUtils;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

/**
 * Author:Skeet
 * Date: 2023/3/24
 **/
@TapConnectorClass("atlas-spec.json")
public class MongodbAtlasConnector extends MongodbConnector {

    private boolean canListCollections = false;

    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        try {
            onStart(connectionContext);
            try (
                    MongodbAtlasTest mongodbAtlasTest = new MongodbAtlasTest(mongoConfig, consumer,mongoClient,connectionOptions)
            ) {
                mongodbAtlasTest.testOneByOne();
            }
        } catch (Throwable throwable) {
            TapLogger.error(TAG,throwable.getMessage());
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, "Failed, " + throwable.getMessage()));
        } finally {
            onStop(connectionContext);
        }
        return connectionOptions;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        if(canListCollections){
            return super.tableCount(connectionContext);
        }
        int index = 0;
        try {
            Document collectionCursor = (Document) mongoDatabase.runCommand(new Document("listCollections",1).append("authorizedCollections",true).append("nameOnly",true)).get("cursor");
            index = 0;
            List<String> collectionNames = new ArrayList<>();
            if(collectionCursor != null){
                collectionNames = ((List<Document>)collectionCursor.get("firstBatch")).stream().map(doc -> doc.get("name").toString()).collect(Collectors.toList());
            }
            for (String collectionName : collectionNames) {
                try {
                    getMongoCollection(collectionName).find().first();
                }catch (Exception e){
                    continue;
                }
                index++;
            }
        } catch (Exception e) {
            exceptionCollector.collectTerminateByServer(e);
            exceptionCollector.collectReadPrivileges(e);
            throw e;
        }
        return index;
    }
    @Override
    protected void getTableNames(TapConnectionContext tapConnectionContext, int batchSize, Consumer<List<String>> listConsumer) throws Throwable {
        if(canListCollections){
            super.getTableNames(tapConnectionContext, batchSize, listConsumer);
            return;
        }
        try {
            Document collectionCursor = (Document) mongoDatabase.runCommand(new Document("listCollections",1).append("authorizedCollections",true).append("nameOnly",true)).get("cursor");
            List<Document> collectionNames = new ArrayList<>();
            if(collectionCursor != null){
                collectionNames = (List<Document>)collectionCursor.get("firstBatch");
            }
            List<String> temp = new ArrayList<>();
            for (Document collection : collectionNames) {
                // 去除视图表
                if (collection.get("type", "").equals("view")) {
                    continue;
                }
                String tableName = collection.getString("name");
                // 如果 tableName 以 "system." 开头, 则跳过(这是一些系统表)
                if (tableName.startsWith("system.")) {
                    continue;
                }
                try {
                    getMongoCollection(tableName).find().first();
                }catch (Exception e){
                    continue;
                }

                temp.add(tableName);
                if (temp.size() >= batchSize) {
                    listConsumer.accept(temp);
                    temp.clear();
                }
            }
            if (!temp.isEmpty()) {
                listConsumer.accept(temp);
                temp.clear();
            }
        }catch (Exception e){
            exceptionCollector.collectTerminateByServer(e);
            exceptionCollector.collectReadPrivileges(e);
            throw e;
        }
    }

    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        final String database = mongoConfig.getDatabase();
        final String version = MongodbUtil.getVersionString(mongoClient, database);
        Document collectionCursor = (Document) mongoDatabase.runCommand(new Document("listCollections",1).append("authorizedCollections",true).append("nameOnly",true)).get("cursor");
        TableFieldTypesGenerator tableFieldTypesGenerator = InstanceFactory.instance(TableFieldTypesGenerator.class);
        this.stringTypeValueMap = new HashMap<>();
        final int sampleSizeBatchSize = getSampleSizeBatchSize(connectionContext);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 30, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(30));

        try (Closeable ignored = executor::shutdown) {
            List<String> collectionNameList = new ArrayList<>();
            if(collectionCursor != null){
                collectionNameList = ((List<Document>)collectionCursor.get("firstBatch")).stream().map(doc -> doc.get("name").toString()).collect(Collectors.toList());
            }
            if (tables != null && !tables.isEmpty()) {
                collectionNameList = ListUtils.retainAll(collectionNameList, tables);
            }
            ListUtils.partition(collectionNameList, tableSize).forEach(nameList -> {
                CountDownLatch countDownLatch = new CountDownLatch(nameList.size());

                if (version.compareTo("3.2") >= 0) {
                    Map<String, MongoCollection<Document>> documentMap = Collections.synchronizedMap(new HashMap<>());

                    nameList.forEach(name -> executor.execute(() -> {
                        documentMap.put(name, mongoDatabase.getCollection(name));
                        countDownLatch.countDown();
                    }));

                    try {
                        countDownLatch.await();
                    } catch (InterruptedException e) {
                        TapLogger.error(TAG, "MongodbConnector discoverSchema countDownLatch await", e);
                    }

                    //List all the tables under the database.
                    List<TapTable> list = list();
                    nameList.forEach(name -> {
                        TapTable table = new TapTable(name);
                        table.defaultPrimaryKeys("_id");
                        MongoCollection<?> collection = documentMap.get(name);
                        try {
                            MongodbUtil.sampleDataRow(collection, sampleSizeBatchSize, (dataRow) -> {
                                Set<String> fieldNames = dataRow.keySet();
                                for (String fieldName : fieldNames) {
                                    BsonValue value = dataRow.get(fieldName);
                                    getRelateDatabaseField(connectionContext, tableFieldTypesGenerator, value, fieldName, table);
                                }
                            });
                        } catch (Exception e) {
                            TapLogger.error(TAG, "Use $sample load mongo connection {}'s {} schema failed {}, will use first row as data schema.",
                                    MongodbUtil.maskUriPassword(mongoConfig.getUri()), name, e.getMessage(), e);
                        }

                        collection.listIndexes().forEach((index) -> {
                            ;
                            TapIndex tapIndex = new TapIndex();
                            // TODO: TapIndex struct not enough to represent index, so we encode index info in name
                            tapIndex.setName("__t__" + ((Document) index).toJson());

                            // add a empty tapIndexField
                            TapIndexField tapIndexField = new TapIndexField();
                            tapIndex.indexField(tapIndexField);
                            TapLogger.info(TAG, "MongodbConnector discoverSchema table: {} index {}", name, ((Document) index).toJson());
                            table.add(tapIndex);
                        });
                        Map<String, Object> sharkedKeys = MongodbUtil.getCollectionSharkedKeys(mongoClient, database, name);
                        MongoShardUtil.saveCollectionStats(table, MongodbUtil.getCollectionStatus(mongoClient, database, name), sharkedKeys);
                        MongodbUtil.getTimeSeriesCollectionStatus(mongoClient, database, name,table);
                        if (!Objects.isNull(table.getNameFieldMap()) && !table.getNameFieldMap().isEmpty()) {
                            list.add(table);
                        }
                    });

                    consumer.accept(list);
                } else {
                    Map<String, MongoCollection<BsonDocument>> documentMap = Collections.synchronizedMap(new HashMap<>());

                    nameList.forEach(name -> executor.execute(() -> {
                        documentMap.put(name, mongoDatabase.getCollection(name, BsonDocument.class));
                        countDownLatch.countDown();
                    }));

                    try {
                        countDownLatch.await();
                    } catch (InterruptedException e) {
                        TapLogger.error(TAG, "MongodbConnector discoverSchema countDownLatch await", e);
                    }

                    //List all the tables under the database.
                    List<TapTable> list = list();
                    nameList.forEach(name -> {
                        TapTable table = new TapTable(name);
                        table.defaultPrimaryKeys(singletonList(COLLECTION_ID_FIELD));
                        // save collection info which include capped info
                        try (MongoCursor<BsonDocument> cursor = documentMap.get(name).find().iterator()) {
                            while (cursor.hasNext()) {
                                final BsonDocument document = cursor.next();
                                for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
                                    final String fieldName = entry.getKey();
                                    final BsonValue value = entry.getValue();
                                    getRelateDatabaseField(connectionContext, tableFieldTypesGenerator, value, fieldName, table);
                                }
                                break;
                            }
                        }
                        Map<String, Object> sharkedKeys = MongodbUtil.getCollectionSharkedKeys(mongoClient, database, name);
                        MongoShardUtil.saveCollectionStats(table, MongodbUtil.getCollectionStatus(mongoClient, database, name), sharkedKeys);
                        MongodbUtil.getTimeSeriesCollectionStatus(mongoClient, database, name,table);
                        if (!Objects.isNull(table.getNameFieldMap()) && !table.getNameFieldMap().isEmpty()) {
                            list.add(table);
                        }
                    });

                    consumer.accept(list);
                }
            });
        }
    }
    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        super.onStart(connectionContext);
        try {
            mongoDatabase.listCollectionNames().iterator();
            this.canListCollections = true;
        }catch (Exception e){
            this.canListCollections = false;
        }
    }

    protected void queryByAdvanceFilter(TapConnectorContext connectorContext, TapAdvanceFilter tapAdvanceFilter, TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        queryByAdvanceFilter(connectorContext, tapAdvanceFilter, table, consumer, false);
    }
}
