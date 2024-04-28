package io.tapdata.mongodb.batch;

import com.mongodb.MongoInterruptedException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Sorts;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.mongodb.MongoBatchOffset;
import io.tapdata.mongodb.MongodbConnector;
import io.tapdata.mongodb.MongodbExceptionCollector;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.mongodb.reader.StreamWithOpLogCollection;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

import static com.mongodb.client.model.Filters.gte;
import static io.tapdata.base.ConnectorBase.insertRecordEvent;
import static io.tapdata.base.ConnectorBase.list;

public interface BatchReadEvent {

    static BatchReadEvent of(ReadParam param) {
        MongodbConfig mongoConfig = param.getMongoConfig();
        TapTable table = param.getTable();
        if (StreamWithOpLogCollection.OP_LOG_DB.equals(mongoConfig.getDatabase()) && StreamWithOpLogCollection.OP_LOG_COLLECTION.equals(table.getName())) {
            param.withSort(new Document("$natural", 1))
                    .withOffsetKey("txnNumber");
            return new OpLogDocumentConverter();
        } else {
            param.withSort(Sorts.ascending(MongodbConnector.COLLECTION_ID_FIELD))
                    .withOffsetKey(MongodbConnector.COLLECTION_ID_FIELD);
            return new NormalDocumentConverter();
        }
    }

    default Map<String, Object> convert(Document document) {
        return (Map<String, Object>) document;
    }

    default FindIterable<Document> findIterable(ReadParam param) {
        String offsetKey = param.getOffsetKey();
        int eventBatchSize = param.getEventBatchSize();
        TapTable table = param.getTable();
        MongodbConfig mongoConfig = param.getMongoConfig();
        Bson sort = param.getSort();
        Object offset = param.getOffset();
        TapConnectorContext connectorContext = param.getConnectorContext();
        Log log = connectorContext.getLog();
        MongoCollection<Document> collection = param.getCollection().collectCollection(table.getId());
        FindIterable<Document> findIterable;
        final int batchSize = eventBatchSize > 0 ? eventBatchSize : 5000;
        if (offset == null) {
            findIterable = collection.find().sort(sort).batchSize(batchSize);
        } else {
            MongoBatchOffset mongoOffset = (MongoBatchOffset) offset;
            Object offsetValue = mongoOffset.value();
            if (offsetValue != null) {
                findIterable = collection.find(queryCondition(offsetKey, offsetValue)).sort(sort)
                        .batchSize(batchSize);
            } else {
                findIterable = collection.find().sort(sort).batchSize(batchSize);
                log.warn("Offset format is illegal {}, no offset value has been found. Final offset will be null to do the batchRead", offset);
            }
        }
        if (mongoConfig.isNoCursorTimeout()) {
            findIterable.noCursorTimeout(true);
        }
        return findIterable;
    }

    default void batchReadCollection(ReadParam param) {
        String offsetKey = param.getOffsetKey();
        TapTable table = param.getTable();
        int eventBatchSize = param.getEventBatchSize();
        BiConsumer<List<TapEvent>, Object> tapReadOffsetConsumer = param.getTapReadOffsetConsumer();
        MongoBatchOffset batchOffset = param.getBatchOffset();
        MongodbExceptionCollector exceptionCollector = param.getExceptionCollector();
        ErrorHandler errorHandler = param.getErrorHandler();
        BooleanSupplier checkAlive = param.getCheckAlive();
		List<TapEvent> tapEvents = list();
		FindIterable<Document> findIterable = findIterable(param);
		Document lastDocument = null;
		try (MongoCursor<Document> mongoCursor = findIterable.iterator()) {
			while (mongoCursor.hasNext()) {
				if (!checkAlive.getAsBoolean()) return;
				lastDocument = mongoCursor.next();
				tapEvents.add(insertRecordEvent(convert(lastDocument), table.getId()));
				if (tapEvents.size() == eventBatchSize) {
					Object value = lastDocument.get(offsetKey);
					batchOffset = new MongoBatchOffset(offsetKey, value);
					tapReadOffsetConsumer.accept(tapEvents, batchOffset);
					tapEvents = list();
				}
			}
			if (tapEvents.isEmpty()) {
				return;
			}
			if (lastDocument != null) {
				Object value = lastDocument.get(offsetKey);
				batchOffset = new MongoBatchOffset(offsetKey, value);
				tapReadOffsetConsumer.accept(tapEvents, batchOffset);
			} else {
				tapReadOffsetConsumer.accept(tapEvents, null);
			}
        } catch (Exception e) {
			if (checkAlive.getAsBoolean() || !(e instanceof MongoInterruptedException)) {
				exceptionCollector.collectTerminateByServer(e);
				exceptionCollector.collectReadPrivileges(e);
				exceptionCollector.revealException(e);
				errorHandler.doHandle(e);
			}
        }
    }

    default Bson queryCondition(String firstPrimaryKey, Object value) {
        return gte(firstPrimaryKey, value);
    }
}