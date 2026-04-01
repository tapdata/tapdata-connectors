package io.tapdata.mongodb.writer.error.handler;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;
import io.tapdata.mongodb.writer.BulkWriteModel;
import io.tapdata.mongodb.writer.error.BulkWriteErrorHandler;
import org.bson.Document;

/**
 * @author Tapdata
 * @Description Handle error code 72: Multi-update operations are not allowed when updating the shard key field
 * This error occurs when using UpdateManyModel on a sharded collection where the update involves shard key fields.
 * The solution is to convert UpdateManyModel to UpdateOneModel.
 * @create 2026-03-27
 **/
public class Code72Handler implements BulkWriteErrorHandler {

    @Override
    public WriteModel<Document> handle(
            BulkWriteModel bulkWriteModel,
            WriteModel<Document> writeModel,
            BulkWriteOptions bulkWriteOptions,
            MongoBulkWriteException mongoBulkWriteException,
            BulkWriteError writeError,
            MongoCollection<Document> collection
    ) {
        // Convert UpdateManyModel to UpdateOneModel to avoid shard key update restriction
        try {
            if (writeModel instanceof UpdateManyModel) {
                UpdateManyModel<Document> updateManyModel = (UpdateManyModel<Document>) writeModel;
                Document filter = (Document) updateManyModel.getFilter();
                Document update = (Document) updateManyModel.getUpdate();
                UpdateOptions options = updateManyModel.getOptions();
                
                // Convert to UpdateOneModel with the same filter, update and options
                return new UpdateOneModel<>(filter, update, options);
            }
        } catch (Throwable ignored) {
            // fall through to return null (can't handle)
        }
        return null;
    }
}

