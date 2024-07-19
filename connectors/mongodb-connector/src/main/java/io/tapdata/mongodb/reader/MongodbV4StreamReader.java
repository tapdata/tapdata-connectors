package io.tapdata.mongodb.reader;

import com.mongodb.*;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.*;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.exception.TapPdkOffsetOutOfLogEx;
import io.tapdata.exception.TapPdkRetryableEx;
import io.tapdata.kit.EmptyKit;
import io.tapdata.mongodb.MongodbUtil;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.mongodb.util.MongodbLookupUtil;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.collections4.MapUtils;
import org.bson.*;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.conversions.Bson;
import org.bson.io.ByteBufferBsonInput;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
    private LinkedBlockingQueue<OffsetEvent>[] logQueueArray;

    public MongodbV4StreamReader setPreImage(boolean isPreImage) {
        this.isPreImage = isPreImage;
        return this;
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
        connectionString = new ConnectionString(mongodbConfig.getUri());
    }

    @Override
    public void read(TapConnectorContext connectorContext, List<String> tableList, Object offset, int eventBatchSize, StreamReadConsumer consumer) {
        openChangeStreamPreAndPostImages(tableList);
        List<Bson> pipeline = singletonList(Aggregates.match(
                Filters.in("ns.coll", tableList)
        ));

        if (this.globalStateMap == null) {
            this.globalStateMap = connectorContext.getGlobalStateMap();
        }
//        pipeline = new ArrayList<>();
//        List<Bson> collList = tableList.stream().map(t -> Filters.eq("ns.coll", t)).collect(Collectors.toList());
//        List<Bson> pipeline1 = asList(Aggregates.match(Filters.or(collList)));
        FullDocument fullDocumentOption = FullDocument.UPDATE_LOOKUP;
        FullDocumentBeforeChange fullDocumentBeforeChangeOption = FullDocumentBeforeChange.WHEN_AVAILABLE;
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
            int numThreads = 8;
            logQueueArray = new LinkedBlockingQueue[numThreads];
            for (int i = 0; i < numThreads; i++) {
                logQueueArray[i] = new LinkedBlockingQueue<>(32);
            }
            ExecutorService executorService = Executors.newWorkStealingPool(numThreads);
            AtomicReference<Throwable> throwableAtomicReference = new AtomicReference<>();
            try (final MongoChangeStreamCursor<ChangeStreamDocument<RawBsonDocument>> streamCursor = changeStream.cursor()) {
                new Thread(() -> {
                    List<TapEvent> events = list();
                    while (running.get()) {
                        try {
                            for (int i = 0; i < numThreads; i++) {
                                OffsetEvent event = logQueueArray[i].take();
                                events.add(event.getEvent());
                                if (events.size() >= eventBatchSize) {
                                    consumer.accept(events, event.getOffset());
                                    events = list();
                                }
                            }
                        } catch (InterruptedException ignored) {
                        }
                    }
                }).start();
                if (EmptyKit.isNotNull(throwableAtomicReference.get())) {
                    throw throwableAtomicReference.get();
                }
                AtomicLong count = new AtomicLong(0);
                while (running.get()) {
                    ChangeStreamDocument<RawBsonDocument> event = streamCursor.tryNext();
                    if (event == null) {
                        TapSimplify.sleep(1000);
                    } else {
                        executorService.submit(() -> {
                            try {
                                emit(event, count, numThreads);
                            } catch (Exception e) {
                                throwableAtomicReference.set(e);
                            }
                        });
                    }
                }
            } catch (Throwable throwable) {
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
                    if (mongoCommandException.getErrorCode() == 286) {
                        throw new TapPdkOffsetOutOfLogEx(connectorContext.getSpecification().getId(), offset, throwable);
                    }

                    if (mongoCommandException.getErrorCode() == 211) {
                        throw new TapPdkRetryableEx(connectorContext.getSpecification().getId(), throwable);
                    }
                }
                //else {
                //TapLogger.warn(TAG,"Read change stream from {}, failed {} " ,MongodbUtil.maskUriPassword(mongodbConfig.getUri()), throwable.getMessage());
                //TapLogger.debug(TAG, "Read change stream from {}, failed {}, error {}", MongodbUtil.maskUriPassword(mongodbConfig.getUri()), throwable.getMessage(), getStackString(throwable));
                //}
            } finally {
                executorService.shutdown();
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

    protected void enqueueTapEvent(TapEvent tapEvent, Object offset, int index) {
        try {
            logQueueArray[index].put(new OffsetEvent(tapEvent, offset));
        } catch (InterruptedException ignore) {
        }
    }

    private void emit(ChangeStreamDocument<RawBsonDocument> event, AtomicLong index, int numThreads) {
        MongoNamespace mongoNamespace = event.getNamespace();

        String collectionName = null;
        if (mongoNamespace != null) {
            collectionName = mongoNamespace.getCollectionName();
        }
        if (collectionName == null) {
            return;
        }
        OperationType operationType = event.getOperationType();
        ByteBuffer byteBuffer = event.getFullDocument().getByteBuffer().asNIO();

        try (
                BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(byteBuffer)))
        ) {
            Document fullDocument = codec.decode(reader, decoderContext);
            Document fullDocumentBeforeChange = null;
            if (EmptyKit.isNotNull(event.getFullDocumentBeforeChange())) {
                ByteBuffer byteBufferBefore = event.getFullDocumentBeforeChange().getByteBuffer().asNIO();
                try (BsonBinaryReader readerBefore = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(byteBufferBefore)))) {
                    fullDocumentBeforeChange = codec.decode(readerBefore, decoderContext);
                }
            }
            if (operationType == OperationType.INSERT) {
                DataMap after = new DataMap();
                after.putAll(fullDocument);
                TapInsertRecordEvent recordEvent = insertRecordEvent(after, collectionName);
                recordEvent.setReferenceTime((long) (event.getClusterTime().getTime()) * 1000);
                enqueueTapEvent(recordEvent, event.getResumeToken(), (int) (index.getAndIncrement() % numThreads));
            } else if (operationType == OperationType.DELETE) {
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
                    if (mongodbConfig.isEnableFillingModifiedData()) {
                        final Map lookupData = MongodbLookupUtil.findDeleteCacheByOid(connectionString, collectionName, documentKey.get("_id"), globalStateMap);
                        recordEvent = deleteDMLEvent(MapUtils.isNotEmpty(lookupData) && lookupData.containsKey("data") && lookupData.get("data") instanceof Map
                                ? (Map<String, Object>) lookupData.get("data") : before, collectionName);
                    } else {
                        recordEvent = deleteDMLEvent(before, collectionName);
                    }

                    recordEvent.setReferenceTime((long) (event.getClusterTime().getTime()) * 1000);
                    enqueueTapEvent(recordEvent, event.getResumeToken(), (int) (index.getAndIncrement() % numThreads));
                } else {
                    TapLogger.warn(TAG, "Document key is null, failed to delete. {}", event);
                }
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
                    } else if (null != updateDescription) {
                        Document decodeDocument = new DocumentCodec().decode(new BsonDocumentReader(event.getDocumentKey()), DecoderContext.builder().build());
                        after.putAll(decodeDocument);
                        if (null != updateDescription.getUpdatedFields()) {
                            decodeDocument = new DocumentCodec().decode(new BsonDocumentReader(updateDescription.getUpdatedFields()), DecoderContext.builder().build());
                            for (String key : decodeDocument.keySet()) {
                                after.put(key, decodeDocument.get(key));
                            }
                        }
                    }

                    TapUpdateRecordEvent recordEvent = updateDMLEvent(before, after, collectionName);
//							Map<String, Object> info = new DataMap();
//							Map<String, Object> unset = new DataMap();
                    List<String> removedFields = new ArrayList<>();
                    if (updateDescription != null) {
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
                    }
//							recordEvent.setInfo(info);
                    recordEvent.setReferenceTime((long) (event.getClusterTime().getTime()) * 1000);
                    recordEvent.setIsReplaceEvent(operationType.equals(OperationType.REPLACE));
                    enqueueTapEvent(recordEvent, event.getResumeToken(), (int) (index.getAndIncrement() % numThreads));
                } else {
                    throw new RuntimeException(String.format("Document key is null, failed to update. %s", event));
                }
            }
        }

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
                } catch (MongoException e) {
                    if (e.getCode() == 26) {
                        mongoDatabase.createCollection(tableName);
                        openChangeStream(tableName);
                        return;
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

}
