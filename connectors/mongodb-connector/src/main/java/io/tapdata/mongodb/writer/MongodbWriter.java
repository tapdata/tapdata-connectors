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
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.mongodb.MongodbUtil;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.mongodb.reader.MongodbV4StreamReader;
import io.tapdata.mongodb.util.MongodbLookupUtil;
import io.tapdata.mongodb.writer.error.BulkWriteErrorCodeHandlerEnum;
import io.tapdata.mongodb.writer.error.IgnoreWriteModel;
import io.tapdata.mongodb.writer.error.TapMongoBulkWriteException;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.utils.AppType;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.bson.Document;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
	private final Map<String, Set<String>> shardKeyMap;
	private String insertPolicy;
	private String updatePolicy;
	private final Map<String, ClientSession> sessionMap;

	// Error file cleanup configuration
	private volatile long lastCleanupTime = 0;
	private static final long CLEANUP_INTERVAL_MS = 60 * 60 * 1000; // 1 hour
	private static final int ERROR_FILE_RETENTION_DAYS = 7; // Keep error files for 7 days
	private static final int ERROR_FILE_MAX_COUNT = 100; // Keep at most 100 error files

	public MongodbWriter(KVMap<Object> globalStateMap, MongodbConfig mongodbConfig, MongoClient mongoClient, Log tapLogger, Map<String, Set<String>> shardKeyMap, Map<String, ClientSession> sessionMap) {
		this.globalStateMap = globalStateMap;
		this.mongoClient = mongoClient;
		this.mongoDatabase = mongoClient.getDatabase(mongodbConfig.getDatabase());
		this.connectionString = new ConnectionString(mongodbConfig.getUri());
		this.mongodbConfig = mongodbConfig;
		this.is_cloud = AppType.currentType().isCloud();
		this.tapLogger = tapLogger;
		this.shardKeyMap = shardKeyMap;
		this.sessionMap = sessionMap;
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
		ClientSession clientSession = sessionMap.get(Thread.currentThread().getName());
		if (Boolean.TRUE.equals(mongodbConfig.getDoubleActive())) {
			if (null != clientSession) {
				doubleActiveWrite(tapRecordEvents, table, writeListResultConsumer, clientSession);
			} else {
				ClientSession session = null;
				try {
					session = mongoClient.startSession();
					session.startTransaction();
					doubleActiveWrite(tapRecordEvents, table, writeListResultConsumer, session);
					session.commitTransaction();
				} catch (Exception e) {
					Optional.ofNullable(session).ifPresent(ClientSession::abortTransaction);
				} finally {
					Optional.ofNullable(session).ifPresent(ClientSession::close);
				}
			}
		} else {
			write(table, tapRecordEvents, writeListResultConsumer, clientSession);
		}
	}

	private void doubleActiveWrite(List<TapRecordEvent> tapRecordEvents, TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, ClientSession session) throws Throwable {
        Document doubleActiveDoc = new Document("_id", "aaaaaaaa");
		UpdateOptions options = new UpdateOptions().upsert(true);
		mongoDatabase.getCollection("_tap_double_active").updateOne(session, doubleActiveDoc, new Document("$set", new Document("ts", System.currentTimeMillis())), options);
		write(table, tapRecordEvents, writeListResultConsumer, session);
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
		boolean errorContextDumped = false;

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
				// Dump error context only once per batch to avoid duplicate files across retries.
				if (!errorContextDumped) {
					try {
						String dumpPath = dumpBulkWriteErrorContext(e, bulkWriteModel, bulkWriteOptions, collection);
						if (dumpPath != null) {
							TapLogger.info("MongoDB bulk write context saved to file: ", dumpPath);
						}
					} catch (Throwable ignored) {
						// Never affect main flow
					}
					errorContextDumped = true;
				}
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
		BulkWriteResult result = collection.bulkWrite(normalWriteMode(inserted, updated, deleted, options, table, pks, tapRecordEvent));
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
		List<Integer> ignoredIndexes = new ArrayList<>();

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
					handledIndexes.add(index);
					if (retryWriteModel == IgnoreWriteModel.INSTANCE) {
						ignoredIndexes.add(index);
					} else {
						retryWriteModels.add(retryWriteModel);
					}
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
						if (ignoredIndexes.contains(i)) {
							// skip ignored index
							continue;
						} else {
							newWriteModelList.add(retryWriteModels.get(0));
							retryWriteModels.remove(0);
						}
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

	protected String dumpBulkWriteErrorContext(MongoBulkWriteException ex,
											   BulkWriteModel bulkWriteModel,
											   BulkWriteOptions bulkWriteOptions,
											   MongoCollection<Document> collection) {
		try {
			String thread = Thread.currentThread().getName();
			String ns = collection != null ? collection.getNamespace().getFullName() : "unknown";
			String database = collection != null ? collection.getNamespace().getDatabaseName() : "unknown";
			String collectionName = collection != null ? collection.getNamespace().getCollectionName() : "unknown";
			String sanitizedThread = thread.replaceAll("[^a-zA-Z0-9_.-]", "_");
			String dateMinute = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm").format(LocalDateTime.now());

			String fileName = "mongodb_bulk_write_error_" + dateMinute + "_" + sanitizedThread + ".log";
			String baseTmp = System.getProperty("java.io.tmpdir", ".");
			Path dir = Paths.get(baseTmp, "tapdata-mongodb-errors");
			String content = buildBulkWriteErrorContent(ex, bulkWriteModel, bulkWriteOptions, ns, database, collectionName, thread, dateMinute);
			try {
				Files.createDirectories(dir);
				Path filePath = dir.resolve(fileName);
				Files.write(filePath, content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
				// Trigger cleanup after successful write
				cleanupErrorFilesIfNeeded(dir);
				return filePath.toAbsolutePath().toString();
			} catch (Throwable t1) {
				// Fallback to current working directory
				String userDir = System.getProperty("user.dir", ".");
				Path fallbackDir = Paths.get(userDir, "tapdata-mongodb-errors");
				try {
					Files.createDirectories(fallbackDir);
					Path filePath = fallbackDir.resolve(fileName);
					Files.write(filePath, content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
					// Trigger cleanup after successful write
					cleanupErrorFilesIfNeeded(fallbackDir);
					return filePath.toAbsolutePath().toString();
				} catch (Throwable ignored) {
					return null;
				}
			}
		} catch (Throwable ignored) {
			return null;
		}
	}

	private String buildBulkWriteErrorContent(MongoBulkWriteException ex,
											  BulkWriteModel bulkWriteModel,
											  BulkWriteOptions bulkWriteOptions,
											  String namespace,
											  String database,
											  String collectionName,
											  String thread,
											  String dateMinute) {
		StringBuilder sb = new StringBuilder(8192);
		sb.append("==== MongoDB Bulk Write Error Dump ====")
				.append('\n');
		sb.append("dateMinute=").append(dateMinute).append(" thread=").append(thread).append('\n');
		sb.append("database=").append(database).append(" collection=").append(collectionName).append('\n');
		sb.append("namespace=").append(namespace).append('\n');
		sb.append("bulkWriteOptions.ordered=").append(bulkWriteOptions != null && bulkWriteOptions.isOrdered()).append('\n');
		sb.append("bulkWriteOptions.toString=").append(bulkWriteOptions).append('\n');
		Map<Integer, List<BulkWriteError>> errorByIndex = new HashMap<>();

		if (ex != null) {
			sb.append("exceptionClass=").append(ex.getClass().getName()).append('\n');
			sb.append("exceptionMessage=").append(ex.getMessage()).append('\n');
			sb.append("serverAddress=").append(ex.getServerAddress()).append('\n');
			sb.append("writeConcernError=").append(ex.getWriteConcernError()).append('\n');
			List<BulkWriteError> errs = ex.getWriteErrors();
			if (errs != null && !errs.isEmpty()) {
				for (BulkWriteError we : errs) {
					errorByIndex.computeIfAbsent(we.getIndex(), k -> new ArrayList<>()).add(we);
				}
			}
			if (!errs.isEmpty()) {
				sb.append("writeErrors(size=").append(errs.size()).append("):").append('\n');
				for (int i = 0; i < errs.size(); i++) {
					BulkWriteError we = errs.get(i);
					sb.append("  [").append(i).append("] index=").append(we.getIndex())
							.append(" code=").append(we.getCode())
							.append(" message=").append(we.getMessage())
							.append(" details=").append(we.getDetails())
							.append('\n');
				}
			}
		}
		// Dump write models
		try {
			List<WriteModel<Document>> allOps = bulkWriteModel != null ? bulkWriteModel.getAllOpWriteModels() : null;
			List<WriteModel<Document>> onlyInserts = bulkWriteModel != null ? bulkWriteModel.getOnlyInsertWriteModels() : null;
			if (allOps != null) {
				sb.append("allOpWriteModels(size=").append(allOps.size()).append("):").append('\n');
				for (int i = 0; i < allOps.size(); i++) {
					sb.append("  [").append(i).append("] ").append(describeWriteModel(allOps.get(i))).append('\n');
					List<BulkWriteError> idxErrs = errorByIndex.get(i);
					if (idxErrs != null && !idxErrs.isEmpty()) {
						for (BulkWriteError we : idxErrs) {
							sb.append("    -> error index=").append(we.getIndex())
									.append(" code=").append(we.getCode())
									.append(" message=").append(we.getMessage())
									.append(" details=").append(we.getDetails())
									.append('\n');
						}
					}
				}
			}
			if (onlyInserts != null) {
				sb.append("onlyInsertWriteModels(size=").append(onlyInserts.size()).append("):").append('\n');
				for (int i = 0; i < onlyInserts.size(); i++) {
					sb.append("  [").append(i).append("] ").append(describeWriteModel(onlyInserts.get(i))).append('\n');
				}
			}
		} catch (Throwable ignored) {
			// ignore model rendering errors
		}
		sb.append("==== END ====").append('\n');
		return sb.toString();
	}


	private String describeWriteModel(WriteModel<Document> model) {
		if (model == null) return "null";
		try {
			if (model instanceof InsertOneModel) {
				InsertOneModel<Document> m = (InsertOneModel<Document>) model;
				return "InsertOneModel document=" + safeToJson(m.getDocument());
			} else if (model instanceof UpdateOneModel) {
				UpdateOneModel<Document> m = (UpdateOneModel<Document>) model;
				return "UpdateOneModel filter=" + safeToJson(m.getFilter()) + " update=" + safeToJson(m.getUpdate()) + " options=" + m.getOptions();
			} else if (model instanceof UpdateManyModel) {
				UpdateManyModel<Document> m = (UpdateManyModel<Document>) model;
				return "UpdateManyModel filter=" + safeToJson(m.getFilter()) + " update=" + safeToJson(m.getUpdate()) + " options=" + m.getOptions();
			} else if (model instanceof ReplaceOneModel) {
				ReplaceOneModel<Document> m = (ReplaceOneModel<Document>) model;
				return "ReplaceOneModel filter=" + safeToJson(m.getFilter()) + " replacement=" + safeToJson(m.getReplacement()) + " options=" + m.getReplaceOptions();
			} else if (model instanceof DeleteOneModel) {
				DeleteOneModel<Document> m = (DeleteOneModel<Document>) model;
				return "DeleteOneModel filter=" + safeToJson(m.getFilter());
			} else if (model instanceof DeleteManyModel) {
				DeleteManyModel<Document> m = (DeleteManyModel<Document>) model;
				return "DeleteManyModel filter=" + safeToJson(m.getFilter());
			} else {
				return String.valueOf(model);
			}
		} catch (Throwable t) {
			return String.valueOf(model);
		}
	}

	private String safeToJson(Object obj) {
		try {
			if (obj == null) return "null";
			if (obj instanceof Document) return ((Document) obj).toJson();
			return String.valueOf(obj);
		} catch (Throwable ignored) {
			return String.valueOf(obj);
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
				List<WriteModel<Document>> writeModels = normalWriteMode(inserted, updated, deleted, options, table, pks, recordEvent);
				if (writeModels != null && !writeModels.isEmpty()) {
					writeModels.forEach(bulkWriteModel::addAnyOpModel);
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

	protected List<WriteModel<Document>> normalWriteMode(AtomicLong inserted, AtomicLong updated, AtomicLong deleted, UpdateOptions options, TapTable tapTable, Collection<String> pks, TapRecordEvent recordEvent) {
		List<WriteModel<Document>> writeModels = new ArrayList<>();
		if (recordEvent instanceof TapInsertRecordEvent) {
			WriteModel<Document> writeModel;
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
				writeModels.add(new UpdateManyModel<>(pkFilter, update, options));
				if (MapUtils.isNotEmpty(unsetDoc)) {
					writeModels.add(new UpdateManyModel<>(pkFilter, new Document("$unset", unsetDoc), options));
				}
			} else {
				if (CollectionUtils.isNotEmpty(pks) && MapUtils.isNotEmpty(unsetDoc)) {
					Document pkFilter = getPkFilter(pks, insertRecordEvent.getAfter());
					Document update = new Document("$set", insertRecordEvent.getAfter());
					writeModels.add(new UpdateManyModel<>(pkFilter, update, options));
					if (MapUtils.isNotEmpty(unsetDoc)) {
						writeModels.add(new UpdateManyModel<>(pkFilter, new Document("$unset", unsetDoc), options));
					}
				} else {
					writeModels.add(new InsertOneModel<>(new Document(insertRecordEvent.getAfter())));
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
				after = DbKit.getAfterForUpdateMongo(after, before, allColumn, pks);
			}
			Map<String, Object> info = recordEvent.getInfo();
			Document pkFilter;
			Document u = new Document();
			if (info != null && info.get("$op") != null) {
				WriteModel<Document> writeModel;
				Object id = info.get("_id");
				id = MongodbUtil.convertValue(id);
				pkFilter = new Document("_id", id);
				((Map<String, Object>) info.get("$op")).forEach((k, v) -> u.put(k, MongodbUtil.convertValue(v)));
				u.remove("$v"); // Exists '$v' in update operation of MongoDB(v3.6), remove it because can't apply in write model.
				boolean isUpdate = u.keySet().stream().anyMatch(k -> k.startsWith("$"));
				if (isUpdate) {
					writeModel = new UpdateManyModel<>(pkFilter, u, options);
					options.upsert(false);
				} else {
					writeModel = new ReplaceOneModel<>(pkFilter, u, new ReplaceOptions().upsert(false));
				}
				writeModels.add(writeModel);
			} else {
				pkFilter = getPkFilter(pks, !before.isEmpty() ? before : after);
				if (updateRecordEvent.getIsReplaceEvent() != null && updateRecordEvent.getIsReplaceEvent()) {
					u.putAll(after);
					writeModels.add(new ReplaceOneModel<>(pkFilter, u, new ReplaceOptions().upsert(false)));
				} else {
					if (ConnectionOptions.DML_UPDATE_POLICY_IGNORE_ON_NON_EXISTS.equals(updatePolicy)) {
						options.upsert(false);
					}
					MongodbUtil.removeIdIfNeed(pks, after);
					u.append("$set", after);
					writeModels.add(new UpdateManyModel<>(pkFilter, u, options));
					Document unsetDoc = wrapUnset(recordEvent);
					if (MapUtils.isNotEmpty(unsetDoc)) {
						writeModels.add(new UpdateManyModel<>(pkFilter, new Document("$unset", unsetDoc), options));
					}
				}
			}
			updated.incrementAndGet();
		} else if (recordEvent instanceof TapDeleteRecordEvent && CollectionUtils.isNotEmpty(pks)) {

			TapDeleteRecordEvent deleteRecordEvent = (TapDeleteRecordEvent) recordEvent;
			Map<String, Object> before = deleteRecordEvent.getBefore();
			final Document pkFilter = getPkFilter(pks, before);

			writeModels.add(new DeleteOneModel<>(pkFilter));
			deleted.incrementAndGet();
		}

		return writeModels;
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

		// 过滤掉被更高层级字段覆盖的字段
		List<String> filteredFields = filterHierarchicalFields(removedFields);

		for (String removeField : filteredFields) {
			// 检查after中是否存在该字段（只检查完全相同的字段，不检查父字段）
			if (!after.containsKey(removeField)) {
				unsetDoc.append(removeField, true);
			}
		}
		return unsetDoc;
	}

	/**
	 * 过滤层级字段，保留最高层级的字段
	 * 例如：[a, a.b, a.c] -> [a]
	 * [a.b, a.c] -> [a.b, a.c]
	 * [a, a.b, a.b.c] -> [a]
	 * [a.b, a.b.c, a.b.e] -> [a.b]
	 */
	private List<String> filterHierarchicalFields(List<String> fields) {
		if (CollectionUtils.isEmpty(fields)) {
			return fields;
		}

		// 按字段名排序，确保父字段在子字段前面
		List<String> sortedFields = new ArrayList<>(fields);
		sortedFields.sort(String::compareTo);

		List<String> result = new ArrayList<>();

		for (String field : sortedFields) {
			boolean shouldAdd = true;

			// 检查是否已经有父字段存在
			for (String existingField : result) {
				if (isChildField(field, existingField)) {
					shouldAdd = false;
					break;
				}
			}

			if (shouldAdd) {
				// 移除所有是当前字段子字段的已存在字段
				result.removeIf(existingField -> isChildField(existingField, field));
				result.add(field);
			}
		}

		return result;
	}

	/**
	 * 判断field是否是parentField的子字段
	 * 例如：isChildField("a.b", "a") -> true
	 * isChildField("a.b.c", "a.b") -> true
	 * isChildField("a", "a.b") -> false
	 */
	private boolean isChildField(String field, String parentField) {
		return field.startsWith(parentField + ".");
	}

	/**
	 * 判断parentField是否是field的父字段
	 * 例如：isParentField("a", "a.b") -> true
	 * isParentField("a.b", "a.b.c") -> true
	 * isParentField("a.b", "a") -> false
	 */
	private boolean isParentField(String parentField, String field) {
		return field.startsWith(parentField + ".");
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

	/**
	 * Cleanup old error files if needed (with frequency limit)
	 * This method is called after each error file write, but actual cleanup only happens once per hour
	 *
	 * @param errorDir The directory containing error files
	 */
	private void cleanupErrorFilesIfNeeded(Path errorDir) {
		long now = System.currentTimeMillis();
		if (now - lastCleanupTime < CLEANUP_INTERVAL_MS) {
			return; // Skip cleanup to avoid frequent execution
		}
		lastCleanupTime = now;
		cleanupErrorFiles(errorDir, ERROR_FILE_RETENTION_DAYS, ERROR_FILE_MAX_COUNT);
	}

	/**
	 * Cleanup error files based on retention days and max file count
	 * Strategy:
	 * 1. First delete files older than retention days
	 * 2. If remaining files still exceed max count, delete oldest files
	 *
	 * @param errorDir      The directory containing error files
	 * @param retentionDays Keep files for this many days
	 * @param maxFiles      Keep at most this many files
	 */
	private void cleanupErrorFiles(Path errorDir, int retentionDays, int maxFiles) {
		try {
			if (!Files.exists(errorDir) || !Files.isDirectory(errorDir)) {
				return;
			}

			long cutoffTime = System.currentTimeMillis() - (retentionDays * 24L * 60 * 60 * 1000);

			List<Path> allFiles = Files.list(errorDir)
					.filter(Files::isRegularFile)
					.filter(p -> p.getFileName().toString().startsWith("mongodb_bulk_write_error_"))
					.collect(java.util.stream.Collectors.toList());

			// Step 1: Delete files older than retention period
			List<Path> validFiles = new ArrayList<>();
			for (Path p : allFiles) {
				try {
					if (Files.getLastModifiedTime(p).toMillis() < cutoffTime) {
						Files.delete(p);
						tapLogger.debug("Deleted old error file (older than {} days): {}", retentionDays, p.getFileName());
					} else {
						validFiles.add(p);
					}
				} catch (IOException e) {
					tapLogger.warn("Failed to delete old error file: {}, error: {}", p, e.getMessage());
				}
			}

			// Step 2: If remaining files still exceed max count, delete oldest files
			if (validFiles.size() > maxFiles) {
				validFiles.sort(Comparator.comparingLong(p -> {
					try {
						return Files.getLastModifiedTime(p).toMillis();
					} catch (IOException e) {
						return 0L;
					}
				}));

				int filesToDelete = validFiles.size() - maxFiles;
				for (int i = 0; i < filesToDelete; i++) {
					try {
						Files.delete(validFiles.get(i));
						tapLogger.debug("Deleted excess error file (exceeds max count {}): {}", maxFiles, validFiles.get(i).getFileName());
					} catch (IOException e) {
						tapLogger.warn("Failed to delete excess error file: {}, error: {}", validFiles.get(i), e.getMessage());
					}
				}
			}

			tapLogger.debug("Error file cleanup completed. Directory: {}, retention days: {}, max files: {}",
					errorDir, retentionDays, maxFiles);
		} catch (Throwable t) {
			// Never affect main flow
			tapLogger.warn("Error during error file cleanup: {}", t.getMessage());
		}
	}

}
