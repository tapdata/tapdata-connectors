package io.tapdata.mongodb.batch;

import com.mongodb.MongoInterruptedException;
import com.mongodb.client.*;
import com.mongodb.client.model.Sorts;
import io.tapdata.common.concurrent.ConcurrentProcessor;
import io.tapdata.common.concurrent.TapExecutors;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.mongodb.MongoBatchOffset;
import io.tapdata.mongodb.MongodbConnector;
import io.tapdata.mongodb.MongodbExceptionCollector;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.mongodb.entity.ReadParam;
import io.tapdata.mongodb.reader.StreamWithOpLogCollection;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.bson.*;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.conversions.Bson;
import org.bson.io.ByteBufferBsonInput;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

import static com.mongodb.client.model.Filters.gte;
import static io.tapdata.base.ConnectorBase.insertRecordEvent;
import static io.tapdata.base.ConnectorBase.list;

public class MongoBatchReader {
    public static final int DEFAULT_BATCH_SIZE = 5000;
    protected BiConsumer<List<TapEvent>, Object> tapReadOffsetConsumer;
    protected MongodbExceptionCollector exceptionCollector;
    protected TapConnectorContext connectorContext;
    protected MongoBatchOffset batchOffset;
    protected BooleanSupplier checkAlive;
    protected ErrorHandler errorHandler;
    protected MongodbConfig mongoConfig;
    protected int eventBatchSize;
    protected String offsetKey;
    protected TapTable table;
    protected Object offset;
    protected Bson sort;

    public static MongoBatchReader of(ReadParam param) {
        return new MongoBatchReader(param);
    }

    public MongoBatchReader(ReadParam param) {
        this.tapReadOffsetConsumer = param.getTapReadOffsetConsumer();
        this.exceptionCollector = param.getExceptionCollector();
        this.connectorContext = param.getConnectorContext();
        this.eventBatchSize = param.getEventBatchSize();
        this.errorHandler = param.getErrorHandler();
        this.batchOffset = param.getBatchOffset();
        this.mongoConfig = param.getMongoConfig();
        this.checkAlive = param.getCheckAlive();
        this.offset = param.getOffset();
        this.table = param.getTable();
        this.table = param.getTable();
        if (StreamWithOpLogCollection.OP_LOG_DB.equals(mongoConfig.getDatabase()) && StreamWithOpLogCollection.OP_LOG_COLLECTION.equals(table.getId())) {
            this.sort = new Document("$natural", 1);
            this.offsetKey = "txnNumber";
        } else {
            this.sort = Sorts.ascending(MongodbConnector.COLLECTION_ID_FIELD);
            this.offsetKey = MongodbConnector.COLLECTION_ID_FIELD;
        }
    }

    protected Map<String, Object> convert(Document document) {
        return document;
    }

    protected FindIterable<RawBsonDocument> findIterable(ReadParam param, ClientSession clientSession) {
//        Log log = connectorContext.getLog();
        MongoCollection<RawBsonDocument> collection = param.getRawCollection().collectRawCollection(table.getId());
        FindIterable<RawBsonDocument> findIterable;
        final int batchSize = eventBatchSize > 0 ? eventBatchSize : DEFAULT_BATCH_SIZE;
        findIterable = collection.find().batchSize(batchSize);
//        if (offset == null) {
//            findIterable = collection.find().sort(sort).batchSize(batchSize);
//        } else {
//            MongoBatchOffset mongoOffset = (MongoBatchOffset) offset;
//            Object offsetValue = mongoOffset.value();
//            if (offsetValue != null) {
//                findIterable = collection.find(queryCondition(offsetKey, offsetValue)).sort(sort)
//                        .batchSize(batchSize);
//            } else {
//                findIterable = collection.find().sort(sort).batchSize(batchSize);
//                log.warn("Offset format is illegal {}, no offset value has been found. Final offset will be null to do the batchRead", offset);
//            }
//        }
        if (mongoConfig.isNoCursorTimeout() && null != clientSession) {
            findIterable = collection.find(clientSession).batchSize(batchSize).noCursorTimeout(true);
        } else {
            findIterable = collection.find().batchSize(batchSize);
        }
        return findIterable;
    }

    public void batchReadCollection(ReadParam param) {
        if (mongoConfig.isNoCursorTimeout()) {
            batchReadCursorNoCursorTimeout(param);
        } else {
            batchReadCursor(findIterable(param, null));
        }
    }

    public void batchReadCursorNoCursorTimeout(ReadParam param) {
        Log log = connectorContext.getLog();
        MongoClient mongoClient = param.getMongoClient();
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        try (ClientSession clientSession = mongoClient.startSession()) {
            BsonDocument bsonDocument = clientSession.getServerSession().getIdentifier();
            BsonArray bsonArray = new BsonArray();
            bsonArray.add(bsonDocument);
            scheduler.scheduleAtFixedRate(() -> {
                mongoClient.getDatabase(mongoConfig.getDatabase()).runCommand(new BsonDocument("refreshSessions", bsonArray));
                log.info("refresh session");
            }, 1, 1, TimeUnit.MINUTES);
            FindIterable<RawBsonDocument> findIterable = findIterable(param, clientSession);
            batchReadCursor(findIterable);
        } finally {
            scheduler.shutdownNow();
        }
    }

    public void batchReadCursor(FindIterable<RawBsonDocument> findIterable) {
        DocumentCodec codec = new DocumentCodec();
        DecoderContext decoderContext = DecoderContext.builder().build();
        AtomicReference<Exception> throwableAtomicReference = new AtomicReference<>();
        ConcurrentProcessor<Object, Object> concurrentProcessor = TapExecutors.createSimple(8, 32, "MongoBatchReader-Processor");
        Thread t = new Thread(() -> {
            List<TapEvent> events = list();
            while (checkAlive.getAsBoolean()) {
                try {
                    Object obj = concurrentProcessor.get(2, TimeUnit.SECONDS);
                    if (EmptyKit.isNotNull(obj)) {
                        if (obj instanceof TapEvent) {
                            events.add((TapEvent) obj);
                            if (events.size() >= eventBatchSize) {
                                tapReadOffsetConsumer.accept(events, new HashMap<>());
                                events = new ArrayList<>();
                            }
                        } else if (obj instanceof CountDownLatch) {
                            if (events.size() > 0) {
                                tapReadOffsetConsumer.accept(events, new HashMap<>());
                                events = new ArrayList<>();
                            }
                            ((CountDownLatch) obj).countDown();
                        }
                    } else {
                        if (events.size() > 0) {
                            tapReadOffsetConsumer.accept(events, new HashMap<>());
                            events = new ArrayList<>();
                        }
                    }
                } catch (Exception e) {
                    throwableAtomicReference.set(e);
                }
            }
        });
        t.setName("MongoBatchReader-Consumer");
        t.start();
        try (MongoCursor<RawBsonDocument> mongoCursor = findIterable.iterator()) {
            while (mongoCursor.hasNext()) {
                if (EmptyKit.isNotNull(throwableAtomicReference.get())) {
                    throw throwableAtomicReference.get();
                }
                RawBsonDocument lastDocument = mongoCursor.next();
                concurrentProcessor.runAsync(lastDocument, e -> {
                    try {
                        ByteBuffer byteBuffer = lastDocument.getByteBuffer().asNIO();
                        try (BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(byteBuffer)))) {
                            return insertRecordEvent(convert(codec.decode(reader, decoderContext)), table.getId());
                        }
                    } catch (Exception er) {
                        throwableAtomicReference.set(er);
                        return null;
                    }
                });
            }
            CountDownLatch countDownLatch = new CountDownLatch(1);
            concurrentProcessor.runAsync(countDownLatch, e -> e);
            while (checkAlive.getAsBoolean()) {
                if (countDownLatch.await(500L, TimeUnit.MILLISECONDS)) {
                    break;
                }
            }
        } catch (Exception e) {
            doException(e);
        } finally {
            concurrentProcessor.close();
        }
    }

    protected synchronized void emit(Document lastDocument, AtomicReference<List<TapEvent>> tapEvents) {
        tapEvents.get().add(insertRecordEvent(convert(lastDocument), table.getId()));
        if (tapEvents.get().size() >= eventBatchSize) {
            tapReadOffsetConsumer.accept(tapEvents.get(), new HashMap<>());
            tapEvents.set(list());
        }
    }

    protected MongoBatchOffset findMongoBatchOffset(Document lastDocument) {
        Object value = lastDocument.get(offsetKey);
        batchOffset = new MongoBatchOffset(offsetKey, value);
        return batchOffset;
    }

    protected void afterEmit(List<TapEvent> tapEvents, Document lastDocument) {
        if (tapEvents.isEmpty()) {
            return;
        }
        Object emitOffset = null;
        if (lastDocument != null) {
            emitOffset = findMongoBatchOffset(lastDocument);
        }
        tapReadOffsetConsumer.accept(tapEvents, emitOffset);
    }

    protected void doException(Exception e) {
        if (!checkAlive.getAsBoolean() && e instanceof MongoInterruptedException) {
            return;
        }
        exceptionCollector.collectTerminateByServer(e);
        exceptionCollector.collectReadPrivileges(e);
        exceptionCollector.revealException(e);
        errorHandler.doHandle(e);
    }

    protected Bson queryCondition(String firstPrimaryKey, Object value) {
        return gte(firstPrimaryKey, value);
    }
}