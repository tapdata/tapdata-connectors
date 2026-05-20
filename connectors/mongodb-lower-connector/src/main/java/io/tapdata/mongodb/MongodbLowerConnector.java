package io.tapdata.mongodb;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.Optional;

/**
 * Author:Skeet
 * Date: 2023/3/24
 **/
@TapConnectorClass("mongo3-spec.json")
public class MongodbLowerConnector extends MongodbConnector {
	@Override
	protected void createIndex(TapTable table, List<TapIndex> indexList, Log log) {
		if (null == indexList || indexList.isEmpty()) return;
		indexList.forEach(index -> {
			log.info("find index: {}" + index.getName());
			try {
				String name = index.getName();
				// 去除 __t__ 前缀
				if (!name.startsWith("__t__")) {
					return;
				}
				name = name.substring(5);
				Document dIndex = Document.parse(name);
				if (dIndex == null) {
					return;
				}
				MongoCollection<Document> targetCollection = mongoDatabase.getCollection(table.getName());
				IndexOptions indexOptions = new IndexOptions();
				// 1. 遍历 index, 生成 indexOptions
				dIndex.forEach((key, value) -> {
					if ("unique".equals(key)) {
						indexOptions.unique((Boolean) value);
					} else if ("sparse".equals(key)) {
						indexOptions.sparse((Boolean) value);
					} else if ("expireAfterSeconds".equals(key)) {
						indexOptions.expireAfter(((Double) value).longValue(), java.util.concurrent.TimeUnit.SECONDS);
					} else if ("background".equals(key)) {
						indexOptions.background((Boolean) value);
					} else if ("partialFilterExpression".equals(key)) {
						indexOptions.partialFilterExpression((Bson) value);
					} else if ("defaultLanguage".equals(key)) {
						indexOptions.defaultLanguage((String) value);
					} else if ("languageOverride".equals(key)) {
						indexOptions.languageOverride((String) value);
					} else if ("textVersion".equals(key)) {
						indexOptions.textVersion((Integer) value);
					} else if ("weights".equals(key)) {
						indexOptions.weights((Bson) value);
					} else if ("sphereVersion".equals(key)) {
						indexOptions.sphereVersion((Integer) value);
					} else if ("bits".equals(key)) {
						indexOptions.bits((Integer) value);
					} else if ("min".equals(key)) {
						indexOptions.min((Double) value);
					} else if ("max".equals(key)) {
						indexOptions.max((Double) value);
					} else if ("bucketSize".equals(key)) {
						indexOptions.bucketSize((Double) value);
					} else if ("storageEngine".equals(key)) {
						indexOptions.storageEngine((Bson) value);
					} else if ("wildcardProjection".equals(key)) {
						indexOptions.wildcardProjection((Bson) value);
					} else if ("hidden".equals(key)) {
						indexOptions.hidden((Boolean) value);
					} else if ("version".equals(key)) {
						indexOptions.version((Integer) value);
					}
				});
				try {
					targetCollection.createIndex(dIndex.get("key", Document.class), indexOptions);
				} catch (Exception ignored) {
					log.warn("create index failed 1: " + ignored.getMessage());
				}
			} catch (Exception ignored) {
				log.warn("create index failed 2: " + ignored.getMessage());
				// TODO: 如果解码失败, 说明这个索引不应该在这里创建, 忽略掉
			}
		});
	}

	@Override
	protected void createIndex(TapConnectorContext tapConnectorContext, TapTable table, TapCreateIndexEvent tapCreateIndexEvent) {
		final List<TapIndex> indexList = tapCreateIndexEvent.getIndexList();
		if (CollectionUtils.isNotEmpty(indexList)) {
			Document keys = new Document();
			for (TapIndex tapIndex : indexList) {
				try {
					final List<TapIndexField> indexFields = tapIndex.getIndexFields();
					if (CollectionUtils.isNotEmpty(indexFields)) {
						if (indexFields.size() == 1 && "_id".equals(indexFields.stream().findFirst().get().getName())) {
							continue;
						}
						final MongoCollection<Document> collection = mongoDatabase.getCollection(table.getName());
						keys = new Document();
						for (TapIndexField indexField : indexFields) {
							keys.append(indexField.getName(), 1);
						}
						final IndexOptions indexOptions = new IndexOptions();
						indexOptions.background(true);
						if (indexFields.size() != 1 || !"_id".equals(indexFields.stream().findFirst().get().getName())) {
							indexOptions.unique(tapIndex.isUnique());
						}
						if (EmptyKit.isNotEmpty(tapIndex.getName())) {
							indexOptions.name(tapIndex.getName());
						}
						collection.createIndex(keys, indexOptions);
					}
				} catch (Exception e) {
					if (e instanceof MongoCommandException) {
						MongoCommandException mongoCommandException = (MongoCommandException) e;
						if (mongoCommandException.getErrorCode() == 86 && "IndexKeySpecsConflict".equals(mongoCommandException.getErrorCodeName())) {
							// Index already exists
							Document finalKeys = keys;
							Optional.ofNullable(tapConnectorContext.getLog()).ifPresent(log -> log.info("Index [{}] already exists, can ignore creating this index, server error detail message: {}", finalKeys, e.getMessage()));
							continue;
						}
						if (mongoCommandException.getErrorCode() == 85 && "IndexOptionsConflict".equals(mongoCommandException.getErrorCodeName())) {
							// Index already exists but options is inconsistent
							Document finalKeys = keys;
							Optional.ofNullable(tapConnectorContext.getLog()).ifPresent(log -> log.warn("Index [{}] already exists but options is inconsistent, will ignore creating this index, server error detail message: {}", finalKeys, e.getMessage()));
							continue;
						}
					}
					exceptionCollector.collectUserPwdInvalid(mongoConfig.getUri(), e);
					exceptionCollector.throwWriteExIfNeed(null, e);
					throw e;
				}
			}
		}
	}
}
