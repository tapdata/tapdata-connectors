package io.tapdata.mongodb.writer.error.handler;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import io.tapdata.mongodb.writer.BulkWriteModel;
import io.tapdata.mongodb.writer.error.BulkWriteErrorHandler;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author samuel
 * @Description Use array filter option to update an element in array, when array field not exists, will occur this error.
 * e.g. Execute the following command in mongo shell will reproduce this error
 * 1. db.test.insert({id: 1})
 * 2. db.test.updateOne({id: 1},{$set: {'arr.$[elem1].fld1': 1,'arr.$[elem1].fld2': 'test'}},{arrayFilters: [{'elem1.fld1': 1}]})
 * Error message: The path 'arr' must exist in the document in order to apply array updates
 * @create 2023-12-08 16:24
 **/
public class Code2Handler implements BulkWriteErrorHandler {

	public static final Pattern PATTERN = Pattern.compile("The path '(.*?)' must exist in the document in order to apply array updates\\.");
	public static final String $_ELEMENT_1 = "$[element1]";

	@Override
	public WriteModel<Document> handle(BulkWriteModel bulkWriteModel, WriteModel<Document> writeModel, BulkWriteOptions bulkWriteOptions, MongoBulkWriteException mongoBulkWriteException, BulkWriteError writeError, MongoCollection<Document> collection) {
		String errorField = getErrorField(writeError, PATTERN, 1);
		if (StringUtils.isBlank(errorField)) {
			return null;
		}
		UpdateOptions updateOptions = updateOptions(writeModel);
		List<? extends Bson> arrayFilters = updateOptions.getArrayFilters();
		if (null == arrayFilters || arrayFilters.isEmpty()) {
			return null;
		}
		Document filter = updateModelFilter(writeModel);
		if (null == filter) {
			return null;
		}
		Document update = updateModelUpdate(writeModel);
		if (!(update.get("$set") instanceof Document)) {
			return null;
		}
		Document setDoc = (Document) update.get("$set");
		if (null == setDoc || setDoc.isEmpty()) {
			return null;
		}
		Document newSetDoc = new Document();
		Document addToSetDoc = new Document();
		for (Map.Entry<String, Object> entry : setDoc.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if (key.contains($_ELEMENT_1)) {
				int index = key.indexOf($_ELEMENT_1);
				addToSetDoc.append(key.substring(index + $_ELEMENT_1.length() + 1), value);
			} else {
				newSetDoc.append(key, value);
			}
		}
		if (newSetDoc.isEmpty() && addToSetDoc.isEmpty()) {
			return null;
		}
		update.clear();
		if (!newSetDoc.isEmpty()) {
			update.append("$set", newSetDoc);
		}
		if (!addToSetDoc.isEmpty()) {
			update.append("$addToSet", new Document(errorField, addToSetDoc));
		}
		updateOptions.arrayFilters(null);

		return writeModel;
	}
}
