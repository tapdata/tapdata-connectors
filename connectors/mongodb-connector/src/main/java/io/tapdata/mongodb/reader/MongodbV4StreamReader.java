package io.tapdata.mongodb.reader;

import com.mongodb.*;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.*;
import io.tapdata.common.concurrent.ConcurrentProcessor;
import io.tapdata.common.concurrent.TapExecutors;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.exception.TapPdkRetryableEx;
import io.tapdata.kit.EmptyKit;
import io.tapdata.mongodb.MongodbExceptionCollector;
import io.tapdata.mongodb.MongodbUtil;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.mongodb.util.MongodbLookupUtil;
import io.tapdata.pdk.apis.consumer.StreamReadOneByOneConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.collections4.MapUtils;
import org.bson.*;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.conversions.Bson;
import org.bson.io.ByteBufferBsonInput;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapdata.base.ConnectorBase.*;
import static java.util.Collections.singletonList;

/**
 * @author jackin
 * @date 2022/5/17 14:34
 **/
public class MongodbV4StreamReader implements MongodbStreamReader {

    public static final String TAG = MongodbV4StreamReader.class.getSimpleName();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private MongodbConfig mongodbConfig;
    private KVMap<Object> globalStateMap;
    private boolean isPreImage;
    private DocumentCodec codec;
    private DecoderContext decoderContext;
    private ConnectionString connectionString;
    private ConcurrentProcessor<ChangeStreamDocument<RawBsonDocument>, OffsetEvent> concurrentProcessor;
    private MongodbExceptionCollector mongodbExceptionCollector;
    private String dropTransactionId;
    private Thread consumeStreamEventThread;

    public MongodbV4StreamReader setPreImage(boolean isPreImage) {
        this.isPreImage = isPreImage;
        return this;
    }

    public MongodbV4StreamReader() {
        this.mongodbExceptionCollector = new MongodbExceptionCollector();
    }

    @Override
    public void onStart(MongodbConfig mongodbConfig) {
        this.mongodbConfig = mongodbConfig;
        if (mongoClient == null) {
            mongoClient = MongodbUtil.createMongoClient(mongodbConfig);
            mongoDatabase = mongoClient.getDatabase(mongodbConfig.getDatabase());
        }
        running.compareAndSet(false, true);
        codec = new DocumentCodec();
        decoderContext = DecoderContext.builder().build();
        concurrentProcessor = TapExecutors.createSimple(8, 32, "MongodbV4StreamReader-Processor");
        connectionString = new ConnectionString(mongodbConfig.getUri());
    }

    @Override
    public void read(TapConnectorContext connectorContext, List<String> tableList, Object offset, StreamReadOneByOneConsumer consumer) throws Exception {
        openChangeStreamPreAndPostImages(tableList);
        if (Boolean.TRUE.equals(mongodbConfig.getDoubleActive())) {
            tableList.add("_tap_double_active");
        }
        List<Bson> pipeline = singletonList(Aggregates.match(
                Filters.in("ns.coll", tableList)
        ));

        if (this.globalStateMap == null) {
            this.globalStateMap = connectorContext.getGlobalStateMap();
        }
//        pipeline = new ArrayList<>();
//        List<Bson> collList = tableList.stream().map(t -> Filters.eq("ns.coll", t)).collect(Collectors.toList());
//        List<Bson> pipeline1 = asList(Aggregates.match(Filters.or(collList)));
        FullDocument fullDocumentOption = FullDocument.DEFAULT;
        FullDocumentBeforeChange fullDocumentBeforeChangeOption = FullDocumentBeforeChange.WHEN_AVAILABLE;
        if (mongodbConfig.isEnableFillingModifiedData() || isPreImage) {
            fullDocumentOption = FullDocument.UPDATE_LOOKUP;
        }
        while (running.get()) {
            ChangeStreamIterable<RawBsonDocument> changeStream;
            if (offset != null) {
                //报错之后， 再watch一遍
                //如果完全没事件， 就需要从当前时间开始watch
                if (offset instanceof Integer) {
                    changeStream = mongoDatabase.watch(pipeline, RawBsonDocument.class).startAtOperationTime(new BsonTimestamp((Integer) offset, 0)).fullDocument(fullDocumentOption);
                } else {
                    changeStream = mongoDatabase.watch(pipeline, RawBsonDocument.class).resumeAfter((BsonDocument) offset).fullDocument(fullDocumentOption);
                }
            } else {
                changeStream = mongoDatabase.watch(pipeline, RawBsonDocument.class).fullDocument(fullDocumentOption);
            }
            if (isPreImage) {
                changeStream.fullDocumentBeforeChange(fullDocumentBeforeChangeOption);
            }
            consumer.streamReadStarted();
            AtomicReference<Exception> throwableAtomicReference = new AtomicReference<>();
            try (final MongoChangeStreamCursor<ChangeStreamDocument<RawBsonDocument>> streamCursor = changeStream.cursor()) {
                consumeStreamEventThread = new Thread(() -> {
                    while (running.get()) {
                        try {
                            OffsetEvent event = concurrentProcessor.get(10, TimeUnit.MILLISECONDS);
                            if (EmptyKit.isNotNull(event)) {
                                consumer.accept(event.getEvent(), event.getOffset());
                            }
                        } catch (Exception e) {
                            throwableAtomicReference.set(e);
                            return;
                        }
                    }
                });
                consumeStreamEventThread.setName("MongodbV4StreamReader-Consumer");
                consumeStreamEventThread.start();
                while (running.get()) {
                    if (EmptyKit.isNotNull(throwableAtomicReference.get())) {
                        throw throwableAtomicReference.get();
                    }
                    ChangeStreamDocument<RawBsonDocument> event = streamCursor.tryNext();
                    if (event == null) {
                        continue;
                    }
                    concurrentProcessor.runAsync(event, e -> {
                        try {
                            return emit(e);
                        } catch (Exception er) {
                            throwableAtomicReference.set(er);
                            return null;
                        }
                    });
                }
            } catch (Exception throwable) {
                if (!running.get()) {
                    final String message = throwable.getMessage();
                    if (throwable instanceof IllegalStateException && EmptyKit.isNotEmpty(message) && (message.contains("state should be: open") || message.contains("Cursor has been closed"))) {
                        return;
                    }
                    if (throwable instanceof MongoInterruptedException) {
                        return;
                    }
                }

                if (throwable instanceof MongoCommandException) {
                    MongoCommandException mongoCommandException = (MongoCommandException) throwable;
                    mongodbExceptionCollector.collectOffsetInvalid(offset, throwable);
                    mongodbExceptionCollector.collectCdcConfigInvalid(throwable);
                    if (mongoCommandException.getErrorCode() == 211) {
                        throw new TapPdkRetryableEx(connectorContext.getSpecification().getId(), throwable);
                    }
                }
                throw throwable;
                //else {
                //TapLogger.warn(TAG,"Read change stream from {}, failed {} " ,MongodbUtil.maskUriPassword(mongodbConfig.getUri()), throwable.getMessage());
                //TapLogger.debug(TAG, "Read change stream from {}, failed {}, error {}", MongodbUtil.maskUriPassword(mongodbConfig.getUri()), throwable.getMessage(), getStackString(throwable));
                //}
            } finally {
                concurrentProcessor.close();
            }
        }
    }

    static class OffsetEvent {
        private final Object offset;
        private final TapEvent event;

        public OffsetEvent(TapEvent event, Object offset) {
            this.offset = offset;
            this.event = event;
        }

        public Object getOffset() {
            return offset;
        }

        public TapEvent getEvent() {
            return event;
        }
    }

    private OffsetEvent emit(ChangeStreamDocument<RawBsonDocument> event) {
        MongoNamespace mongoNamespace = event.getNamespace();
        String collectionName = null;
        if (mongoNamespace != null) {
            collectionName = mongoNamespace.getCollectionName();
        }
        if (collectionName == null) {
            return null;
        }
        BsonDocument transactionDocument = event.getLsid();
        //双活情形下，需要过滤_tap_double_active记录的同事务数据
        if (Boolean.TRUE.equals(mongodbConfig.getDoubleActive()) && EmptyKit.isNotNull(transactionDocument)) {
            String transactionId = transactionDocument.getBinary("id").asUuid().toString();
            if ("_tap_double_active".equals(collectionName)) {
                dropTransactionId = transactionId;
                return null;
            } else {
                if (null != dropTransactionId) {
                    if (dropTransactionId.equals(transactionId)) {
                        return null;
                    } else {
                        dropTransactionId = null;
                    }
                }
            }
        }
        OffsetEvent offsetEvent = null;
        OperationType operationType = event.getOperationType();
        Document fullDocumentBeforeChange = null;
        if (EmptyKit.isNotNull(event.getFullDocumentBeforeChange())) {
            ByteBuffer byteBufferBefore = event.getFullDocumentBeforeChange().getByteBuffer().asNIO();
            try (BsonBinaryReader readerBefore = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(byteBufferBefore)))) {
                fullDocumentBeforeChange = codec.decode(readerBefore, decoderContext);
            }
        }
        if (null == event.getFullDocument()) {
            if (operationType == OperationType.DELETE) {
                DataMap before = new DataMap();
                if (event.getDocumentKey() != null) {
                    final Document documentKey = new DocumentCodec().decode(new BsonDocumentReader(event.getDocumentKey()), DecoderContext.builder().build());
                    if (isPreImage && MapUtils.isNotEmpty(fullDocumentBeforeChange)) {
                        before.putAll(fullDocumentBeforeChange);
                    } else {
                        before.put("_id", documentKey.get("_id"));
                    }
                    // Queries take a long time and are disabled when not needed, QPS went down from 4000 to 400.
                    // If you need other field data in delete event can't be disabled.
                    TapDeleteRecordEvent recordEvent;
                    if (mongodbConfig.isEnableFillingModifiedData() && !isPreImage) {
                        final Map lookupData = MongodbLookupUtil.findDeleteCacheByOid(connectionString, collectionName, documentKey.get("_id"), globalStateMap);
                        recordEvent = deleteDMLEvent(MapUtils.isNotEmpty(lookupData) && lookupData.containsKey("data") && lookupData.get("data") instanceof Map
                                ? (Map<String, Object>) lookupData.get("data") : before, collectionName);
                    } else {
                        recordEvent = deleteDMLEvent(before, collectionName);
                    }

                    recordEvent.setReferenceTime((long) (event.getClusterTime().getTime()) * 1000);
                    offsetEvent = new OffsetEvent(recordEvent, event.getResumeToken());
                } else {
                    TapLogger.warn(TAG, "Document key is null, failed to delete. {}", event);
                }
            } else if (operationType == OperationType.UPDATE) {
                UpdateDescription updateDescription = event.getUpdateDescription();
                if (null == event.getDocumentKey()) {
                    throw new RuntimeException(String.format("Document key is null, failed to update. %s", event));
                } else if (null == updateDescription) {
                    throw new RuntimeException(String.format("UpdateDescription key is null, failed to update. %s", event));
                }

                DataMap before = new DataMap();
                DataMap after = new DataMap();
                if (isPreImage && MapUtils.isNotEmpty(fullDocumentBeforeChange)) {
                    before.putAll(fullDocumentBeforeChange);
                }
                Document decodeDocument = new DocumentCodec().decode(new BsonDocumentReader(event.getDocumentKey()), DecoderContext.builder().build());
                after.putAll(decodeDocument);

                if (null != updateDescription.getUpdatedFields()) {
                    Document decodeUpdateDocument = new DocumentCodec().decode(new BsonDocumentReader(updateDescription.getUpdatedFields()), DecoderContext.builder().build());
                    decodeUpdateDocument.forEach((k, v) -> {
                        if (k.contains(".")) {
                            return;
                        }
                        after.put(k, v);
                    });
					before.forEach((k, v) -> {
						if (!after.containsKey(k)) {
							after.put(k, v);
						}
					});
                }

                TapUpdateRecordEvent recordEvent = updateDMLEvent(before, after, collectionName);
                Map<String, Object> info = new DataMap();

                List<String> removedFields = new ArrayList<>();
                if (null != updateDescription.getRemovedFields()) {
                    for (String f : updateDescription.getRemovedFields()) {

                        if (after.keySet().stream().noneMatch(v -> v.equals(f))) {
                            removedFields.add(f);
                        }
                    }
                }
                if (!removedFields.isEmpty()) {
                    recordEvent.removedFields(removedFields);
                }

                LinkedHashSet<String> updatedFields = new LinkedHashSet<>();
                BsonDocument bsonUpdatedFields = updateDescription.getUpdatedFields();
                if (null != bsonUpdatedFields) {
                    updatedFields.addAll(bsonUpdatedFields.keySet());
                }

                // 双活场景依赖此属性，区分是否为业务数据修改
                info.put("updatedFields", updatedFields);
                recordEvent.setInfo(info);
                recordEvent.setReferenceTime((long) (event.getClusterTime().getTime()) * 1000);
                recordEvent.setIsReplaceEvent(false);
                offsetEvent = new OffsetEvent(recordEvent, event.getResumeToken());
            }
        } else {
            ByteBuffer byteBuffer = event.getFullDocument().getByteBuffer().asNIO();

            try (
                    BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(byteBuffer)))
            ) {
                Document fullDocument = codec.decode(reader, decoderContext);
                if (operationType == OperationType.INSERT) {
                    DataMap after = new DataMap();
                    after.putAll(fullDocument);
                    TapInsertRecordEvent recordEvent = insertRecordEvent(after, collectionName);
                    recordEvent.setReferenceTime((long) (event.getClusterTime().getTime()) * 1000);
                    offsetEvent = new OffsetEvent(recordEvent, event.getResumeToken());
                } else if (operationType == OperationType.UPDATE || operationType == OperationType.REPLACE) {
                    DataMap before = new DataMap();
                    DataMap after = new DataMap();
                    if (event.getDocumentKey() != null) {
                        UpdateDescription updateDescription = event.getUpdateDescription();
                        if (isPreImage && MapUtils.isNotEmpty(fullDocumentBeforeChange)) {
                            before.putAll(fullDocumentBeforeChange);
                        }
                        if (MapUtils.isNotEmpty(fullDocument)) {
                            after.putAll(fullDocument);
                        } else {
                            Document decodeDocument = new DocumentCodec().decode(new BsonDocumentReader(event.getDocumentKey()), DecoderContext.builder().build());
                            after.putAll(decodeDocument);
                        }
                        if (null != updateDescription && null != updateDescription.getUpdatedFields()) {
                            Document decodeDocument = new DocumentCodec().decode(new BsonDocumentReader(updateDescription.getUpdatedFields()), DecoderContext.builder().build());
                            decodeDocument.forEach((k, v) -> {
                                if (k.contains(".")) {
                                    return;
                                }
                                after.put(k, v);
                            });
                        }

                        TapUpdateRecordEvent recordEvent = updateDMLEvent(before, after, collectionName);
                        Map<String, Object> info = new DataMap();
//							Map<String, Object> unset = new DataMap();
                        List<String> removedFields = new ArrayList<>();

                        LinkedHashSet<String> updatedFields = new LinkedHashSet<>();
                        if (operationType == OperationType.REPLACE) {
                            updatedFields.addAll(fullDocument.keySet());
                        } else if (updateDescription != null) {
                            for (String f : updateDescription.getRemovedFields()) {

                                if (after.keySet().stream().noneMatch(v -> v.equals(f))) {
//										unset.put(f, true);
                                    removedFields.add(f);
                                }
//									if (!after.containsKey(f)) {
//										unset.put(f, true);
//									}
                            }
//								if (unset.size() > 0) {
//									info.put("$unset", unset);
//								}
                            if (removedFields.size() > 0) {
                                recordEvent.removedFields(removedFields);
                            }

                            BsonDocument bsonUpdatedFields = updateDescription.getUpdatedFields();
                            if (null != bsonUpdatedFields) {
                                updatedFields.addAll(bsonUpdatedFields.keySet());
                            }
                        }

                        // 双活场景依赖此属性，区分是否为业务数据修改
                        info.put("updatedFields", updatedFields);
                        recordEvent.setInfo(info);
                        recordEvent.setReferenceTime((long) (event.getClusterTime().getTime()) * 1000);
                        recordEvent.setIsReplaceEvent(operationType.equals(OperationType.REPLACE));
                        offsetEvent = new OffsetEvent(recordEvent, event.getResumeToken());
                    } else {
                        throw new RuntimeException(String.format("Document key is null, failed to update. %s", event));
                    }
                }
            }

        }
        if (null != offsetEvent && offsetEvent.getEvent() instanceof TapRecordEvent) {
            ((TapRecordEvent) offsetEvent.getEvent()).setExactlyOnceId(event.getResumeToken().toString());
        }
        return offsetEvent;
    }

    @Override
    public Object streamOffset(Long offsetStartTime) {
        if (null == offsetStartTime) {
            offsetStartTime = MongodbUtil.mongodbServerTimestamp(mongoDatabase);
        }
        return (int) (offsetStartTime / 1000);
    }

    @Override
    public void onDestroy() {
        running.compareAndSet(true, false);
        if (consumeStreamEventThread != null) {
            consumeStreamEventThread.interrupt();
        }
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }

    protected void openChangeStreamPreAndPostImages(List<String> tableList) {
        if (isPreImage) {
            for (String tableName : tableList) {
                try {
                    openChangeStream(tableName);
                    TapLogger.debug(TAG, "Successfully enabled changeStreamPreAndPostImages for collection: {}", tableName);
                } catch (MongoException e) {
                    if (e.getCode() == 26) {
                        // Collection doesn't exist, create it and try again
                        mongoDatabase.createCollection(tableName);
                        openChangeStream(tableName);
                        TapLogger.debug(TAG, "Created collection and enabled changeStreamPreAndPostImages for: {}", tableName);
                        return;
                    } else if (e.getCode() == 13) {
                        // Authorization error - check if preImage is already enabled
                        TapLogger.debug(TAG, "No permission to enable changeStreamPreAndPostImages for collection: {}, checking if already enabled", tableName);
                        if (isPreImageAlreadyEnabled(tableName)) {
                            TapLogger.info(TAG, "Collection {} already has changeStreamPreAndPostImages enabled, skipping", tableName);
                            continue;
                        } else {
                            TapLogger.warn(TAG, "Collection {} does not have changeStreamPreAndPostImages enabled and no permission to enable it. " +
                                    "Please manually enable preImage for this collection or grant collMod permission to the user.", tableName);
                            continue;
                        }
                    }
                    throw new MongoException(tableName + " failed to enable changeStreamPreAndPostImages", e);
                }
            }
        }
    }

    private void openChangeStream(String tableName) {
        BsonDocument bsonDocument = new BsonDocument();
        bsonDocument.put("collMod", new BsonString(tableName));
        bsonDocument.put("changeStreamPreAndPostImages", new BsonDocument("enabled", new BsonBoolean(true)));
        mongoDatabase.runCommand(bsonDocument);
    }

    /**
     * Check if changeStreamPreAndPostImages is already enabled for the collection
     * @param tableName the collection name to check
     * @return true if preImage is enabled, false otherwise
     */
    private boolean isPreImageAlreadyEnabled(String tableName) {
        try {
            BsonDocument listCollectionsCommand = new BsonDocument();
            listCollectionsCommand.put("listCollections", new BsonInt32(1));
            listCollectionsCommand.put("filter", new BsonDocument("name", new BsonString(tableName)));

            Document result = mongoDatabase.runCommand(listCollectionsCommand);
            Document cursor = result.get("cursor", Document.class);

            if (cursor != null) {
                @SuppressWarnings("unchecked")
                List<Document> firstBatch = (List<Document>) cursor.get("firstBatch");
                if (firstBatch != null && !firstBatch.isEmpty()) {
                    Document collectionInfo = firstBatch.get(0);
                    Document options = collectionInfo.get("options", Document.class);
                    if (options != null) {
                        Document changeStreamPreAndPostImages = options.get("changeStreamPreAndPostImages", Document.class);
                        if (changeStreamPreAndPostImages != null) {
                            Boolean enabled = changeStreamPreAndPostImages.getBoolean("enabled");
                            return Boolean.TRUE.equals(enabled);
                        }
                    }
                }
            }
            return false;
        } catch (MongoException e) {
            TapLogger.warn(TAG, "Failed to check preImage status for collection {}: {}. Assuming not enabled.", tableName, e.getMessage());
            return false;
        }
    }

}
