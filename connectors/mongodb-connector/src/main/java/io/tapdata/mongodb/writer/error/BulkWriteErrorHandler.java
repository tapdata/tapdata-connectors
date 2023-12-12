package io.tapdata.mongodb.writer.error;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.WriteError;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import io.tapdata.mongodb.writer.BulkWriteModel;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author samuel
 * @Description
 * @create 2023-04-23 18:58
 **/
public interface BulkWriteErrorHandler {
	/**
	 * Handle mongodb bulk write error, the external will write again according to the returned write model and options
	 * Will match the corresponding BulkWriteErrorCodeHandler according to the code in the error{@link BulkWriteErrorCodeHandlerEnum},and call the handle method of the corresponding implementation class
	 *
	 * @param bulkWriteModel          Bulk write model{@link BulkWriteModel}
	 * @param writeModel              Error write models{@link WriteModel}
	 * @param bulkWriteOptions        Bulk write options{@link BulkWriteOptions}
	 * @param mongoBulkWriteException Bulk write exception{@link MongoBulkWriteException}
	 * @param writeError              Bulk write error{@link BulkWriteError}
	 * @return Retry write model{@link WriteModel}, if you cannot handle this error, return null
	 */
	WriteModel<Document> handle(
			BulkWriteModel bulkWriteModel,
			WriteModel<Document> writeModel,
			BulkWriteOptions bulkWriteOptions,
			MongoBulkWriteException mongoBulkWriteException,
			BulkWriteError writeError,
			MongoCollection<Document> collection
	);

	/**
	 * Find error field in WriteError's message, will use pattern.matcher() method
	 *
	 * @param writeError {@link WriteError}
	 * @param pattern    Match pattern. e.g. Cannot create field '(.*?)' in element \{(.*?): null}
	 * @param groupIndex After matcher, return which group. e.g. 2
	 * @return Error field name, "" present not found a field
	 */
	default String getErrorField(WriteError writeError, Pattern pattern, int groupIndex) {
		if (null == writeError || StringUtils.isBlank(writeError.getMessage())) {
			return null;
		}
		Matcher matcher = pattern.matcher(writeError.getMessage());
		if (matcher.find() && matcher.groupCount() >= 1) {
			return matcher.group(groupIndex);
		} else {
			return "";
		}
	}

	default Document updateModelFilter(WriteModel<Document> writeModel) {
		if (null == writeModel) {
			return null;
		}
		if (writeModel instanceof UpdateOneModel) {
			return (Document) ((UpdateOneModel<Document>) writeModel).getFilter();
		}
		if (writeModel instanceof UpdateManyModel) {
			return (Document) ((UpdateManyModel<Document>) writeModel).getFilter();
		}
		if (writeModel instanceof ReplaceOneModel) {
			return (Document) ((ReplaceOneModel<Document>) writeModel).getFilter();
		}
		return null;
	}

	default Document updateModelUpdate(WriteModel<Document> writeModel) {
		if (null == writeModel) {
			return null;
		}
		if (writeModel instanceof UpdateOneModel) {
			return (Document) ((UpdateOneModel<Document>) writeModel).getUpdate();
		}
		if (writeModel instanceof UpdateManyModel) {
			return (Document) ((UpdateManyModel<Document>) writeModel).getUpdate();
		}
		if (writeModel instanceof ReplaceOneModel) {
			return ((ReplaceOneModel<Document>) writeModel).getReplacement();
		}
		return null;
	}

	default UpdateOptions updateOptions(WriteModel<Document> writeModel) {
		if (null == writeModel) {
			return null;
		}
		if (writeModel instanceof UpdateOneModel) {
			return ((UpdateOneModel<Document>) writeModel).getOptions();
		}
		if (writeModel instanceof UpdateManyModel) {
			return ((UpdateManyModel<Document>) writeModel).getOptions();
		}
		return null;
	}
}
