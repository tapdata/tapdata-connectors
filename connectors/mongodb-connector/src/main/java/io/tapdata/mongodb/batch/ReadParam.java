package io.tapdata.mongodb.batch;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.mongodb.MongoBatchOffset;
import io.tapdata.mongodb.MongodbExceptionCollector;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

public class ReadParam {
    public static ReadParam of() {
        return new ReadParam();
    }
    Bson sort;
    public ReadParam withSort(Bson sort) {
        this.sort = sort;
        return this;
    }
    String offsetKey;
    public ReadParam withOffsetKey(String offsetKey) {
        this.offsetKey = offsetKey;
        return this;
    }
    TapConnectorContext connectorContext;
    public ReadParam withConnectorContext(TapConnectorContext connectorContext) {
        this.connectorContext = connectorContext;
        return this;
    }
    TapTable table;
    public ReadParam withTapTable(TapTable table) {
        this.table = table;
        return this;
    }
    Object offset;
    public ReadParam withOffset(Object offset) {
        this.offset = offset;
        return this;
    }
    int eventBatchSize;
    public ReadParam withEventBatchSize(int eventBatchSize) {
        this.eventBatchSize = eventBatchSize;
        return this;
    }
    BiConsumer<List<TapEvent>, Object> tapReadOffsetConsumer;
    public ReadParam withTapReadOffsetConsumer(BiConsumer<List<TapEvent>, Object> tapReadOffsetConsumer) {
        this.tapReadOffsetConsumer = tapReadOffsetConsumer;
        return this;
    }
    CollectionCollector collection;
    public ReadParam withMongoCollection(CollectionCollector collection) {
        this.collection = collection;
        return this;
    }
    MongodbConfig mongoConfig;
    public ReadParam withMongodbConfig(MongodbConfig mongoConfig) {
        this.mongoConfig = mongoConfig;
        return this;
    }
    MongoBatchOffset batchOffset;
    public ReadParam withBatchOffset(MongoBatchOffset batchOffset) {
        this.batchOffset = batchOffset;
        return this;
    }
    MongodbExceptionCollector exceptionCollector;
    public ReadParam withMongodbExceptionCollector(MongodbExceptionCollector exceptionCollector) {
        this.exceptionCollector = exceptionCollector;
        return this;
    }
    ErrorHandler errorHandler;
    public ReadParam withErrorHandler(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
        return this;
    }
    BooleanSupplier checkAlive;
    public ReadParam withCheckAlive(BooleanSupplier checkAlive) {
        this.checkAlive = checkAlive;
        return this;
    }

    public Bson getSort() {
        return sort;
    }

    public String getOffsetKey() {
        return offsetKey;
    }

    public TapConnectorContext getConnectorContext() {
        return connectorContext;
    }

    public TapTable getTable() {
        return table;
    }

    public Object getOffset() {
        return offset;
    }

    public int getEventBatchSize() {
        return eventBatchSize;
    }

    public BiConsumer<List<TapEvent>, Object> getTapReadOffsetConsumer() {
        return tapReadOffsetConsumer;
    }

    public CollectionCollector getCollection() {
        return collection;
    }

    public MongodbConfig getMongoConfig() {
        return mongoConfig;
    }

    public MongoBatchOffset getBatchOffset() {
        return batchOffset;
    }

    public MongodbExceptionCollector getExceptionCollector() {
        return exceptionCollector;
    }

    public ErrorHandler getErrorHandler() {
        return errorHandler;
    }

    public BooleanSupplier getCheckAlive() {
        return checkAlive;
    }
}
