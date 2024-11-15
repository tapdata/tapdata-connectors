package io.tapdata.mongodb.writer;

import com.mongodb.ConnectionString;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.mongodb.MongodbUtil;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.mongodb.reader.MongodbV4StreamReader;
import io.tapdata.mongodb.util.MongodbLookupUtil;
import io.tapdata.mongodb.writer.error.BulkWriteErrorCodeHandlerEnum;
import io.tapdata.mongodb.writer.error.TapMongoBulkWriteException;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.utils.AppType;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.writeListResult;

/**
 * @author jackin
 * @date 2022/5/17 18:30
 **/
public class MongodbWriter {

	public static final String TAG = MongodbV4StreamReader.class.getSimpleName();
	protected MongoClient mongoClient;
	private MongoDatabase mongoDatabase;
	private KVMap<Object> globalStateMap;
	private ConnectionString connectionString;
	private MongodbConfig mongodbConfig;
	private final Log tapLogger;

	private boolean is_cloud;
	private final Map<String,Set<String>> shardKeyMap;
	private String insertPolicy;
	private String updatePolicy;

	public MongodbWriter(KVMap<Object> globalStateMap, MongodbConfig mongodbConfig, MongoClient mongoClient, Log tapLogger, Map<String,Set<String>> shardKeyMap) {
		this.globalStateMap = globalStateMap;
		this.mongoClient = mongoClient;
		this.mongoDatabase = mongoClient.getDatabase(mongodbConfig.getDatabase());
		this.connectionString = new ConnectionString(mongodbConfig.getUri());
		this.mongodbConfig = mongodbConfig;
		this.is_cloud = AppType.currentType().isCloud();
		this.tapLogger = tapLogger;
		this.shardKeyMap = shardKeyMap;
	}

	/**
	 * The method invocation life circle is below,
	 * initiated ->
	 * if(needCreateTable)
	 * createTable
	 * if(needClearTable)
	 * clearTable
	 * if(needDropTable)
	 * dropTable
	 * writeRecord
	 * -> destroy -> ended
	 *
	 * @param tapRecordEvents
	 * @param writeListResultConsumer
	 */
	public void writeRecord(List<TapRecordEvent> tapRecordEvents, TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
		if (CollectionUtils.isEmpty(tapRecordEvents)) {
			return;
		}
		if (Boolean.TRUE.equals(mongodbConfig.getDoubleActive())) {
			try (ClientSession session = mongoClient.startSession()) {
				session.startTransaction();
				Document doubleActiveDoc = new Document("_id", "aaaaaaaa");
				UpdateOptions options = new UpdateOptions().upsert(true);
				mongoDatabase.getCollection("_tap_double_active").updateOne(session, doubleActiveDoc, new Document("$set", new Document("ts", System.currentTimeMillis())), options);
				write(table, tapRecordEvents, writeListResultConsumer, session);
				session.commitTransaction();
			}
		} else {
			write(table, tapRecordEvents, writeListResultConsumer, null);
		}
	}

	private void write(TapTable table, List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, ClientSession session) throws Throwable {
		AtomicLong inserted = new AtomicLong(0); //insert count
		AtomicLong updated = new AtomicLong(0); //update count
		AtomicLong deleted = new AtomicLong(0); //delete count

		WriteListResult<TapRecordEvent> writeListResult = writeListResult();
		MongoCollection<Document> collection = getMongoCollection(table.getId());

		Object pksCache = table.primaryKeys(true);
		if (null == pksCache) pksCache = table.primaryKeys();
		final Collection<String> pks = (Collection<String>) pksCache;

		removeOidIfNeed(tapRecordEvents, pks);

		// daas data will cache local
		if (!is_cloud && mongodbConfig.isEnableSaveDeleteData()) {
			MongodbLookupUtil.lookUpAndSaveDeleteMessage(tapRecordEvents, this.globalStateMap, this.connectionString, pks, collection);
		}
		BulkWriteModel bulkWriteModel = buildBulkWriteModel(tapRecordEvents, table, inserted, updated, deleted, pks);

		if (bulkWriteModel.isEmpty()) {
			throw new RuntimeException("Bulk write data failed, write model list is empty, received record size: " + tapRecordEvents.size());
		}

		BulkWriteOptions bulkWriteOptions;
		AtomicReference<RuntimeException> mongoBulkWriteException = new AtomicReference<>();
		while (!bulkWriteModel.isEmpty()) {
			bulkWriteOptions = buildBulkWriteOptions(bulkWriteModel);
			try {
				List<WriteModel<Document>> writeModels = bulkWriteModel.getWriteModels();
				if (EmptyKit.isNotNull(session)) {
					collection.bulkWrite(session, writeModels, bulkWriteOptions);
				} else {
					collection.bulkWrite(writeModels, bulkWriteOptions);
				}
				bulkWriteModel.clearAll();
			} catch (MongoBulkWriteException e) {
				Consumer<RuntimeException> errorConsumer = mongoBulkWriteException::set;
				if (!handleBulkWriteError(e, bulkWriteModel, bulkWriteOptions, collection, errorConsumer)) {
					if (null != mongoBulkWriteException.get()) {
						throw mongoBulkWriteException.get();
					} else {
						throw e;
					}
				}
			}
		}

		//Need to tell incremental engine the write result
		writeListResultConsumer.accept(writeListResult
				.insertedCount(inserted.get())
				.modifiedCount(updated.get())
				.removedCount(deleted.get()));
	}

	protected void removeOidIfNeed(List<TapRecordEvent> tapRecordEvents, Collection<String> pks) {
		if (null == tapRecordEvents || null == pks) {
			return;
		}
		if (pks.contains("_id")) {
			return;
		}
		// remove _id in after
		for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
			Object mergeInfoObj = tapRecordEvent.getInfo(MergeInfo.EVENT_INFO_KEY);
			if (mergeInfoObj instanceof MergeInfo) {
				MergeInfo mergeInfo = (MergeInfo) mergeInfoObj;
				if (mergeInfo.getLevel() > 1) {
					continue;
				}
			}
			Map<String, Object> after = null;
			if (tapRecordEvent instanceof TapInsertRecordEvent) {
				after = ((TapInsertRecordEvent) tapRecordEvent).getAfter();
			} else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
				after = ((TapUpdateRecordEvent) tapRecordEvent).getAfter();
			}
			if (null == after) {
				continue;
			}
			after.remove("_id");
		}
	}

	public void writeUpdateRecordWithLog(TapRecordEvent tapRecordEvent, TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
		if (null == tapRecordEvent) {
			return;
		}
		AtomicLong inserted = new AtomicLong(0); //insert count
		AtomicLong updated = new AtomicLong(0); //update count
		AtomicLong deleted = new AtomicLong(0); //delete count
		WriteListResult<TapRecordEvent> writeListResult = writeListResult();
		Object pksCache = table.primaryKeys(true);
		if (null == pksCache) pksCache = table.primaryKeys();
		final Collection<String> pks = (Collection<String>) pksCache;
		UpdateOptions options = new UpdateOptions().upsert(false);
		MongoCollection<Document> collection = getMongoCollection(table.getId());
		BulkWriteResult result = collection.bulkWrite(Collections.singletonList(normalWriteMode(inserted, updated, deleted, options, table, pks, tapRecordEvent)));
		if (result.getMatchedCount() <= 0) {
			tapLogger.info("update record ignored: {}", tapRecordEvent);
		}
		writeListResultConsumer.accept(writeListResult
			.insertedCount(0)
			.modifiedCount(1)
			.removedCount(0));
	}

	private boolean handleBulkWriteError(
		MongoBulkWriteException originMongoBulkWriteException,
		BulkWriteModel bulkWriteModel,
		BulkWriteOptions bulkWriteOptions,
		MongoCollection<Document> collection,
		Consumer<RuntimeException> errorConsumer
	) {
		List<BulkWriteError> writeErrors = originMongoBulkWriteException.getWriteErrors();
		List<BulkWriteError> cantHandleErrors = new ArrayList<>();
		List<WriteModel<Document>> retryWriteModels = new ArrayList<>();
		List<Integer> handledIndexes = new ArrayList<>();
		for (BulkWriteError writeError : writeErrors) {
			int code = writeError.getCode();
			int index = writeError.getIndex();
			WriteModel<Document> writeModel = bulkWriteModel.getWriteModels().get(index);
			BulkWriteErrorCodeHandlerEnum bulkWriteErrorCodeHandlerEnum = BulkWriteErrorCodeHandlerEnum.fromCode(code);
			if (null != bulkWriteErrorCodeHandlerEnum && null != bulkWriteErrorCodeHandlerEnum.getBulkWriteErrorHandler()) {
				WriteModel<Document> retryWriteModel = null;
				try {
					retryWriteModel = bulkWriteErrorCodeHandlerEnum.getBulkWriteErrorHandler().handle(bulkWriteModel, writeModel, bulkWriteOptions, originMongoBulkWriteException, writeError, collection);
				} catch (Exception ignored) {
				}
				if (null != retryWriteModel) {
					retryWriteModels.add(retryWriteModel);
					handledIndexes.add(index);
				} else {
					cantHandleErrors.add(writeError);
				}
			} else {
				cantHandleErrors.add(writeError);
			}
		}
		if (CollectionUtils.isNotEmpty(cantHandleErrors)) {
			// Keep errors that cannot handle
			MongoBulkWriteException mongoBulkWriteException = new MongoBulkWriteException(
				originMongoBulkWriteException.getWriteResult(),
				cantHandleErrors,
				originMongoBulkWriteException.getWriteConcernError(),
				originMongoBulkWriteException.getServerAddress(),
				originMongoBulkWriteException.getErrorLabels()
			);
			List<WriteModel<Document>> errorWriteModels = new ArrayList<>();
			cantHandleErrors.forEach(writeError -> errorWriteModels.add(bulkWriteModel.getWriteModels().get(writeError.getIndex())));
			TapMongoBulkWriteException tapMongoBulkWriteException = new TapMongoBulkWriteException(mongoBulkWriteException, errorWriteModels);
			errorConsumer.accept(tapMongoBulkWriteException);
			return false;
		} else {
			if (bulkWriteOptions.isOrdered()) {
				List<WriteModel<Document>> newWriteModelList = new ArrayList<>();
				for (int i = 0; i < bulkWriteModel.getAllOpWriteModels().size(); i++) {
					if (i < handledIndexes.get(0)) {
						continue;
					}
					if (handledIndexes.contains(i)) {
						newWriteModelList.add(retryWriteModels.get(0));
						retryWriteModels.remove(0);
					} else {
						newWriteModelList.add(bulkWriteModel.getAllOpWriteModels().get(i));
					}
				}
				bulkWriteModel.clearAll();
				newWriteModelList.forEach(bulkWriteModel::addAnyOpModel);
			} else {
				bulkWriteModel.clearAll();
				retryWriteModels.forEach(bulkWriteModel::addAnyOpModel);
			}
			return true;
		}
	}

	private BulkWriteModel buildBulkWriteModel(List<TapRecordEvent> tapRecordEvents, TapTable table, AtomicLong inserted, AtomicLong updated, AtomicLong deleted, Collection<String> pks) {
		BulkWriteModel bulkWriteModel = new BulkWriteModel(pks.contains("_id"));
		for (TapRecordEvent recordEvent : tapRecordEvents) {
			if (!(recordEvent instanceof TapInsertRecordEvent)) {
				bulkWriteModel.setAllInsert(false);
			}
			UpdateOptions options = new UpdateOptions().upsert(true);
			final Map<String, Object> info = recordEvent.getInfo();
			if (MapUtils.isNotEmpty(info) && info.containsKey(MergeInfo.EVENT_INFO_KEY)) {
				bulkWriteModel.setAllInsert(false);
				final List<WriteModel<Document>> mergeWriteModels = MongodbMergeOperate.merge(inserted, updated, deleted, recordEvent);
				if (CollectionUtils.isNotEmpty(mergeWriteModels)) {
					mergeWriteModels.forEach(bulkWriteModel::addAnyOpModel);
				}
			} else {
				WriteModel<Document> writeModel = normalWriteMode(inserted, updated, deleted, options, table, pks, recordEvent);
				if (writeModel != null) {
					bulkWriteModel.addAnyOpModel(writeModel);
				}
			}
		}
		return bulkWriteModel;
	}

	private static BulkWriteOptions buildBulkWriteOptions(BulkWriteModel bulkWriteModel) {
		BulkWriteOptions bulkWriteOptions = new BulkWriteOptions();
		if (bulkWriteModel.isAllInsert()) {
			bulkWriteOptions.ordered(false);
		} else {
			bulkWriteOptions.ordered(true);
		}
		return bulkWriteOptions;
	}

	protected WriteModel<Document> normalWriteMode(AtomicLong inserted, AtomicLong updated, AtomicLong deleted, UpdateOptions options, TapTable tapTable, Collection<String> pks, TapRecordEvent recordEvent) {
		WriteModel<Document> writeModel = null;
		if (recordEvent instanceof TapInsertRecordEvent) {
			TapInsertRecordEvent insertRecordEvent = (TapInsertRecordEvent) recordEvent;
			Document unsetDoc = wrapUnset(recordEvent);

			if (CollectionUtils.isNotEmpty(pks) && !ConnectionOptions.DML_INSERT_POLICY_JUST_INSERT.equals(insertPolicy)) {
				final Document pkFilter = getPkFilter(pks, insertRecordEvent.getAfter());
				String operation = "$set";
				if (ConnectionOptions.DML_INSERT_POLICY_IGNORE_ON_EXISTS.equals(insertPolicy)) {
					operation = "$setOnInsert";
				}
				if (shardKeyMap.containsKey(tapTable.getId())) {
					Map<String, Object> record = insertRecordEvent.getAfter();
					Set<String> shardKeySet = shardKeyMap.get(tapTable.getId());
					if (CollectionUtils.isNotEmpty(shardKeySet)) {
						shardKeySet.forEach(shardKey -> {
							if (record.containsKey(shardKey)) pkFilter.append(shardKey, record.get(shardKey));
						});
					}
				}
				MongodbUtil.removeIdIfNeed(pks, insertRecordEvent.getAfter());
				Document update = new Document(operation, insertRecordEvent.getAfter());
				if (MapUtils.isNotEmpty(unsetDoc)) {
					update.append("$unset", unsetDoc);
				}
				writeModel = new UpdateManyModel<>(pkFilter, update, options);
			} else {
				if (CollectionUtils.isNotEmpty(pks) && MapUtils.isNotEmpty(unsetDoc)) {
					Document pkFilter = getPkFilter(pks, insertRecordEvent.getAfter());
					Document update = new Document("$set", insertRecordEvent.getAfter())
							.append("$unset", unsetDoc);
					writeModel = new UpdateManyModel<>(pkFilter, update, options);
				} else {
					writeModel = new InsertOneModel<>(new Document(insertRecordEvent.getAfter()));
				}
			}
			inserted.incrementAndGet();
		} else if (recordEvent instanceof TapUpdateRecordEvent && CollectionUtils.isNotEmpty(pks)) {
			Collection<String> allColumn = tapTable.getNameFieldMap().keySet();
			TapUpdateRecordEvent updateRecordEvent = (TapUpdateRecordEvent) recordEvent;
			Map<String, Object> after = updateRecordEvent.getAfter();
            Map<String, Object> before = updateRecordEvent.getBefore();
            before = DbKit.getBeforeForUpdate(after, before, allColumn, pks);
            if (!((TapUpdateRecordEvent) recordEvent).getIsReplaceEvent()) {
                after = DbKit.getAfterForUpdate(after, before, allColumn, pks);
            }
			Map<String, Object> info = recordEvent.getInfo();
			Document pkFilter;
			Document u = new Document();
			if (info != null && info.get("$op") != null) {
				pkFilter = new Document("_id", info.get("_id"));
				u.putAll((Map<String, Object>) info.get("$op"));
				u.remove("$v"); // Exists '$v' in update operation of MongoDB(v3.6), remove it because can't apply in write model.
				boolean isUpdate = u.keySet().stream().anyMatch(k -> k.startsWith("$"));
				if (isUpdate) {
					writeModel = new UpdateManyModel<>(pkFilter, u, options);
					options.upsert(false);
				} else {
					writeModel = new ReplaceOneModel<>(pkFilter, u, new ReplaceOptions().upsert(false));
				}
			} else {
				pkFilter = getPkFilter(pks, before != null && !before.isEmpty() ? before : after);
				if(updateRecordEvent.getIsReplaceEvent() != null && updateRecordEvent.getIsReplaceEvent()){
					u.putAll(after);
					writeModel = new ReplaceOneModel<>(pkFilter, u, new ReplaceOptions().upsert(false));
				}else{
					if (ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS.equals(updatePolicy)) {
						options.upsert(false);
					}
					MongodbUtil.removeIdIfNeed(pks, after);
					u.append("$set", after);
					Document unsetDoc = wrapUnset(recordEvent);
					if (MapUtils.isNotEmpty(unsetDoc)) {
						u.append("$unset", unsetDoc);
					}
					writeModel = new UpdateManyModel<>(pkFilter, u, options);
				}
			}
			updated.incrementAndGet();
		} else if (recordEvent instanceof TapDeleteRecordEvent && CollectionUtils.isNotEmpty(pks)) {

			TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
			Map<String, Object> before = deleteRecordEvent.getBefore();
			final Document pkFilter = getPkFilter(pks, before);

			writeModel = new DeleteOneModel<>(pkFilter);
			deleted.incrementAndGet();
		}

		return writeModel;
	}

	protected Document wrapUnset(TapRecordEvent tapRecordEvent) {
		List<String> removedFields = null;
		Map<String, Object> after = null;
		if (tapRecordEvent instanceof TapInsertRecordEvent) {
			removedFields = ((TapInsertRecordEvent) tapRecordEvent).getRemovedFields();
			after = ((TapInsertRecordEvent) tapRecordEvent).getAfter();
		} else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
			removedFields = ((TapUpdateRecordEvent) tapRecordEvent).getRemovedFields();
			after = ((TapUpdateRecordEvent) tapRecordEvent).getAfter();
		}
		if (CollectionUtils.isEmpty(removedFields) || MapUtils.isEmpty(after)) {
			return null;
		}
		Document unsetDoc = new Document();
		for (String removeField : removedFields) {
			if (after.keySet().stream().noneMatch(v -> v.equals(removeField) || v.startsWith(removeField + ".") || removeField.startsWith(v + "."))) {
				unsetDoc.append(removeField, true);
			}
		}
		return unsetDoc;
	}

	public void onDestroy() {
		if (mongoClient != null) {
			mongoClient.close();
		}
	}

	private MongoCollection<Document> getMongoCollection(String table) {
		return mongoDatabase.getCollection(table);
	}

	private Document getPkFilter(Collection<String> pks, Map<String, Object> record) {
		Document filter = new Document();
		for (String pk : pks) {
			if (!record.containsKey(pk)) {
				// 这个判断是因为存在业务上分片键，mongodb在删除的时候数据只有_id。导致在删除的时候其他键就是空，就会报异常抛出。为了解决这个问题
				// 所以增加兼容
				if (pks.contains("_id") && record.containsKey("_id")) {
					continue;
				}
				throw new RuntimeException("Set filter clause failed, unique key \"" + pk + "\" not exists in data: " + record);
			}
			filter.append(pk, record.get(pk));
		}

		return filter;
	}

	public MongodbWriter dmlPolicy(String insertPolicy, String updatePolicy) {
		this.insertPolicy = insertPolicy;
		this.updatePolicy = updatePolicy;
		return this;
	}

}
