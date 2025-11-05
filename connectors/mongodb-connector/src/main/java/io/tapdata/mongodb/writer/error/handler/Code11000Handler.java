package io.tapdata.mongodb.writer.error.handler;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import io.tapdata.mongodb.util.MapUtil;
import io.tapdata.mongodb.writer.BulkWriteModel;
import io.tapdata.mongodb.writer.error.BulkWriteErrorHandler;
import io.tapdata.mongodb.writer.error.IgnoreWriteModel;
import org.bson.Document;

/**
 * @author samuel
 * @Description Duplicate key error
 * @create 2023-04-23 19:10
 **/
public class Code11000Handler implements BulkWriteErrorHandler {

	@Override
	public WriteModel<Document> handle(
			BulkWriteModel bulkWriteModel,
			WriteModel<Document> writeModel,
			BulkWriteOptions bulkWriteOptions,
			MongoBulkWriteException mongoBulkWriteException,
			BulkWriteError writeError,
			MongoCollection<Document> collection
	) {
		// When duplicate key error occurs, check by filter whether the target document already exists.
		// If exists, mark current operation to be ignored (no retry, no error)
		try {
			Document filter = updateModelFilter(writeModel);
			Document update = updateModelUpdate(writeModel);
			if (filter == null || filter.isEmpty() || update == null || update.isEmpty() || collection == null) {
				return null;
			}
			Object set = update.get("$set");
			if (set instanceof Document) {
				update = (Document) set;
			}
			if (update.isEmpty()) {
				return null;
			}
			filter = new Document(filter);
			for (String key : filter.keySet()) {
				Object value = MapUtil.getValueByKey(update, key);
				if (null == value) {
					return null;
				}
				filter.put(key, value);
			}
			Document existed = collection.find(filter).limit(1).first();
			if (existed != null) {
				// Mark as ignored
				return IgnoreWriteModel.INSTANCE;
			}
		} catch (Throwable ignored) {
			// fall through to return null (can't handle)
		}
		return null;
	}
}
