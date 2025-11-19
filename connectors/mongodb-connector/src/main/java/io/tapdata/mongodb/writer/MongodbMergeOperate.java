package io.tapdata.mongodb.writer;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.WriteModel;
import io.tapdata.common.utils.MergeUtils;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.kit.EmptyKit;
import io.tapdata.mongodb.entity.MergeBundle;
import io.tapdata.mongodb.entity.MergeFilter;
import io.tapdata.mongodb.entity.MergeResult;
import io.tapdata.mongodb.merge.MergeFilterManager;
import io.tapdata.mongodb.util.MapUtil;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.pdk.apis.entity.merge.MergeLookupResult;
import io.tapdata.pdk.apis.entity.merge.MergeTableProperties;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jackin
 * @date 2022/5/30 16:56
 **/
public class MongodbMergeOperate {

	protected static final String UNSET_KEY = "$unset";

	public static List<WriteModel<Document>> merge(AtomicLong inserted, AtomicLong updated, AtomicLong deleted, TapRecordEvent tapRecordEvent) {
		List<WriteModel<Document>> writeModels;
		try {
			writeModels = new ArrayList<>();
			final MergeBundle mergeBundle = mergeBundle(tapRecordEvent);
			final Map<String, Object> info = tapRecordEvent.getInfo();
			if (tapRecordEvent instanceof TapInsertRecordEvent) {
				inserted.incrementAndGet();
			} else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
				updated.incrementAndGet();
			} else if (tapRecordEvent instanceof TapDeleteRecordEvent) {
				deleted.incrementAndGet();
			}
			if (MapUtils.isNotEmpty(info) && info.containsKey(MergeInfo.EVENT_INFO_KEY)) {

				List<MergeResult> mergeResults = new ArrayList<>();
				final MergeInfo mergeInfo = (MergeInfo) info.get(MergeInfo.EVENT_INFO_KEY);
				final MergeTableProperties currentProperty = mergeInfo.getCurrentProperty();
				final List<MergeLookupResult> mergeLookupResults = mergeInfo.getMergeLookupResults();
				Set<String> sharedJoinKeys = mergeInfo.getSharedJoinKeys();
				Map<String, MergeInfo.UpdateJoinKey> updateJoinKeys = mergeInfo.getUpdateJoinKeys();
				Integer level = mergeInfo.getLevel();
				MergeFilter mergeFilter;
				if (level.compareTo(1) > 0) {
					mergeFilter = new MergeFilter(true);
				} else {
					mergeFilter = new MergeFilter(false);
				}
				recursiveMerge(mergeBundle,
						currentProperty,
						mergeResults,
						mergeLookupResults,
						updateJoinKeys,
						sharedJoinKeys,
						mergeFilter,
						level
				);

				if (CollectionUtils.isNotEmpty(mergeResults)) {
					for (MergeResult mergeResult : mergeResults) {
						final MergeResult.Operation operation = mergeResult.getOperation();
						switch (operation) {
							case INSERT:
								writeModels.add(new InsertOneModel<>(mergeResult.getInsert()));
								break;
							case UPDATE:
								writeModels.add(new UpdateManyModel<>(mergeResult.getFilter(), mergeResult.getUpdate(), mergeResult.getUpdateOptions()));
								break;
							case DELETE:
								writeModels.add(new DeleteOneModel<>(mergeResult.getFilter()));
								break;
						}
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(String.format("tap event %s merge failed %s", tapRecordEvent, e.getMessage()), e);
		}
		return writeModels;
	}

	public static void recursiveMerge(
			MergeBundle mergeBundle,
			MergeTableProperties properties,
			List<MergeResult> mergeResults,
			List<MergeLookupResult> mergeLookupResults,
			Map<String, MergeInfo.UpdateJoinKey> updateJoinKeys,
			Set<String> sharedJoinKeys,
			MergeFilter mergeFilter,
			int topLevel
	) {
		recursiveMerge(
				mergeBundle,
				properties,
				mergeResults,
				mergeLookupResults,
				new MergeResult(),
				null,
				updateJoinKeys,
				sharedJoinKeys,
				null,
				mergeFilter,
				topLevel,
				1
		);
	}

	public static void recursiveMerge(
			MergeBundle mergeBundle,
			MergeTableProperties properties,
			List<MergeResult> mergeResults,
			List<MergeLookupResult> mergeLookupResults,
			MergeResult mergeResult,
			MergeResult unsetResult,
			Map<String, MergeInfo.UpdateJoinKey> updateJoinKeys,
			Set<String> sharedJoinKeys,
			MergeTableProperties parentProperties,
			MergeFilter mergeFilter,
			int topLevel,
			int loopTime
	) {
		boolean unsetResultNull = null == unsetResult;
		switch (properties.getMergeType()) {
			case updateOrInsert:
				upsertMerge(mergeBundle, properties, mergeResult);
				if (!mergeFilter.isAppend() && mergeFilter.getFilters().isEmpty()) {
					mergeFilter.addFilter(filter(mergeBundle.getAfter(), properties.getJoinKeys()));
				}
				break;
			case updateWrite:
				unsetResult = updateWriteUnsetMerge(mergeBundle, properties, updateJoinKeys, unsetResult, sharedJoinKeys, mergeFilter, topLevel);
				if (unsetResultNull) {
					addUnsetMerge(mergeResults, unsetResult);
				}
				mergeResult = addMergeResults(mergeResults, mergeResult);
				mergeResult = updateMergeIfNeed(mergeBundle, properties, mergeResult, sharedJoinKeys, mergeFilter);
				break;
			case updateIntoArray:
				unsetResult = updateIntoArrayUnsetMerge(mergeBundle, properties, updateJoinKeys, unsetResult, parentProperties, mergeFilter, loopTime);
				if (unsetResultNull && addUnsetMerge(mergeResults, unsetResult)) {
					mergeBundle.setOperation(MergeBundle.EventOperation.INSERT);
				}
				mergeResult = addMergeResults(mergeResults, mergeResult);
				mergeResult = updateIntoArrayMergeIfNeed(mergeBundle, properties, mergeResult, mergeFilter);
				break;
			default:
				break;
		}

		boolean recursiveOnce = false;
		int privateLoopTime = 0;
		if (CollectionUtils.isNotEmpty(mergeLookupResults)) {
			for (MergeLookupResult mergeLookupResult : mergeLookupResults) {
				privateLoopTime++;
				final Map<String, Object> data = mergeLookupResult.getData();
				mergeBundle = new MergeBundle(MergeBundle.EventOperation.INSERT, null, data);
				mergeBundle.setRecursive(true);
				mergeBundle.setDataExists(mergeLookupResult.isDataExists());
				if (mergeFilter.isAppend() && null != mergeResult) {
					mergeFilter.addFilter(mergeResult.getFilter());
				}
				recursiveMerge(
						mergeBundle,
						mergeLookupResult.getProperty(),
						mergeResults,
						mergeLookupResult.getMergeLookupResults(),
						recursiveOnce ? null : mergeResult,
						unsetResult,
						updateJoinKeys,
						mergeLookupResult.getSharedJoinKeys(),
						properties,
						mergeFilter,
						topLevel,
						privateLoopTime
				);
				recursiveOnce = true;
			}
			return;
		}

		if (mergeResult != null) {
			mergeResults.add(mergeResult);
		}
		if (mergeFilter.isAppend()) {
			mergeFilter.removeLast();
		}
	}

	private static MergeResult updateIntoArrayMergeIfNeed(MergeBundle mergeBundle, MergeTableProperties properties, MergeResult mergeResult, MergeFilter mergeFilter) {
		if (mergeBundle.isDataExists()) {
			updateIntoArrayMerge(mergeBundle, properties, mergeResult, mergeFilter);
		} else {
			mergeResult = null;
		}
		return mergeResult;
	}

	private static MergeResult updateMergeIfNeed(MergeBundle mergeBundle, MergeTableProperties properties, MergeResult mergeResult, Set<String> sharedJoinKeys, MergeFilter mergeFilter) {
		if (mergeBundle.isDataExists()) {
			updateMerge(mergeBundle, properties, mergeResult, sharedJoinKeys, mergeFilter);
		} else {
			mergeResult = null;
		}
		return mergeResult;
	}

	private static boolean addUnsetMerge(List<MergeResult> mergeResults, MergeResult unsetResult) {
		if (null != unsetResult && null != unsetResult.getOperation()) {
			mergeResults.add(unsetResult);
			return true;
		}
		return false;
	}

	private static MergeResult addMergeResults(List<MergeResult> mergeResults, MergeResult mergeResult) {
		if (null == mergeResult) {
			mergeResult = new MergeResult();
		}
		if (mergeResult.getOperation() != null) {
			mergeResults.add(mergeResult);
			mergeResult = new MergeResult();
		}
		return mergeResult;
	}

	private static MergeResult updateWriteUnsetMerge(
			MergeBundle mergeBundle, MergeTableProperties currentProperty,
			Map<String, MergeInfo.UpdateJoinKey> updateJoinKeys,
			MergeResult mergeResult, Set<String> sharedJoinKeys, MergeFilter mergeFilter, int topLevel) {
		if (null == currentProperty) {
			return mergeResult;
		}
		String id = currentProperty.getId();
		if (null == mergeResult && (MapUtils.isEmpty(updateJoinKeys) || !updateJoinKeys.containsKey(id))) {
			return mergeResult;
		}
		MergeBundle.EventOperation operation = mergeBundle.getOperation();
		if (MergeBundle.EventOperation.UPDATE != operation
				&& (MergeBundle.EventOperation.INSERT != operation || !mergeBundle.isRecursive())) {
			return mergeResult;
		}
		Map<String, Object> after = mergeBundle.getAfter();
		String targetPath = currentProperty.getTargetPath();
		boolean isArray = currentProperty.getIsArray();
		String arrayPath = currentProperty.getArrayPath();
		boolean firstMergeResult = false;
		if (null == mergeResult) {
			firstMergeResult = true;
			MergeInfo.UpdateJoinKey updateJoinKey = updateJoinKeys.get(id);
			Map<String, Object> updateJoinKeyAfter = updateJoinKey.getAfter();
			Map<String, Object> updateJoinKeyBefore = updateJoinKey.getBefore();
			List<Map<String, String>> joinKeys = currentProperty.getJoinKeys();
			Document filter;
			mergeResult = new MergeResult();
			filter = unsetFilter(updateJoinKeyBefore, updateJoinKeyAfter, joinKeys, topLevel);
			if (null != updateJoinKey.getParentBefore()) {
				filter.putAll(updateJoinKey.getParentBefore());
			}
			if (isArray) {
				List<Document> arrayFilter = arrayFilter(
						updateJoinKeyBefore,
						joinKeys,
						arrayPath
				);
				mergeResult.getUpdateOptions().arrayFilters(arrayFilter);
			}
			if (EmptyKit.isEmpty(filter)) {
				return mergeResult;
			}
			mergeResult.getFilter().putAll(filter);
		}
		appendAllParentMergeFilters(mergeResult, mergeFilter);

		if (null == mergeResult.getOperation()) {
			mergeResult.setOperation(MergeResult.Operation.UPDATE);
		}
		Document unsetDoc = buildUnsetDocument(sharedJoinKeys, after, targetPath, isArray, firstMergeResult);
		Document update = mergeResult.getUpdate();
		if (update.containsKey(UNSET_KEY)) {
			update.get(UNSET_KEY, Document.class).putAll(unsetDoc);
		} else {
			update.append(UNSET_KEY, unsetDoc);
		}

		return mergeResult;
	}

	public static MergeResult updateIntoArrayUnsetMerge(
			MergeBundle mergeBundle, MergeTableProperties currentProperty,
			Map<String, MergeInfo.UpdateJoinKey> updateJoinKeys, MergeResult mergeResult,
			MergeTableProperties parentProperties, MergeFilter mergeFilter, int loopTime) {
		String id = currentProperty.getId();
		if (loopTime != 1) {
			return null;
		}
		if (null == mergeResult && (MapUtils.isEmpty(updateJoinKeys) || !updateJoinKeys.containsKey(id))) {
			return mergeResult;
		}
		if (null != parentProperties && MergeTableProperties.MergeType.updateIntoArray.equals(parentProperties.getMergeType())) {
			mergeBundle.setOperation(MergeBundle.EventOperation.INSERT);
			return null;
		} else if (null != parentProperties && MergeTableProperties.MergeType.updateWrite.equals(parentProperties.getMergeType())) {
			mergeBundle.setOperation(MergeBundle.EventOperation.INSERT);
			String targetPath = currentProperty.getTargetPath();
			if (StringUtils.isNotBlank(targetPath)) {
				Document update = null == mergeResult ? new Document() : mergeResult.getUpdate();
				Document unsetDoc = new Document(targetPath, true);
				if (update.containsKey(UNSET_KEY)) {
					update.get(UNSET_KEY, Document.class).putAll(unsetDoc);
				} else {
					update.put(UNSET_KEY, unsetDoc);
				}
				return mergeResult;
			} else {
				return null;
			}
		}
		MergeInfo.UpdateJoinKey updateJoinKey = updateJoinKeys.get(id);
		if (null == updateJoinKey) {
			return mergeResult;
		}
		MergeBundle.EventOperation operation = mergeBundle.getOperation();
		if (MergeBundle.EventOperation.UPDATE != operation
				&& (MergeBundle.EventOperation.INSERT != operation || !mergeBundle.isRecursive())) {
			return mergeResult;
		}
		String targetPath = currentProperty.getTargetPath();
		boolean array = currentProperty.getIsArray();
		List<String> arrayKeys = currentProperty.getArrayKeys();
		Map<String, Object> updateJoinKeyBefore = updateJoinKey.getBefore();
		if (null == mergeResult) {
			mergeResult = new MergeResult();
		}
		if (array) {
			List<Document> arrayFilter = arrayFilter(
					updateJoinKeyBefore,
					currentProperty.getJoinKeys(),
					currentProperty.getArrayPath());
			mergeResult.getUpdateOptions().arrayFilters(arrayFilter);
		} else {
			Document filter = filter(updateJoinKeyBefore, currentProperty.getJoinKeys());
			if (null != updateJoinKey.getParentBefore()) {
				filter.putAll(updateJoinKey.getParentBefore());
			}
			mergeResult.getFilter().putAll(filter);
		}
		appendAllParentMergeFilters(mergeResult, mergeFilter);

		if (mergeResult.getOperation() == null) {
			mergeResult.setOperation(MergeResult.Operation.UPDATE);
		}
		Document updateOpDoc;
		if (StringUtils.isNotBlank(targetPath)) {
			updateOpDoc = new Document(targetPath, new ArrayList<>());
			if (mergeResult.getUpdate().containsKey("$set")) {
				mergeResult.getUpdate().get("$set", Document.class).putAll(updateOpDoc);
			} else {
				mergeResult.getUpdate().put("$set", updateOpDoc);
			}
		}
		return mergeResult;
	}

	public static void upsertMerge(MergeBundle mergeBundle, MergeTableProperties currentProperty, MergeResult mergeResult) {
		final String targetPath = currentProperty.getTargetPath();
		final MergeBundle.EventOperation operation = mergeBundle.getOperation();
		Map<String, Object> before = mergeBundle.getBefore();
		Map<String, Object> after = mergeBundle.getAfter();
		Map<String, Object> filterMap;
		if (null != before) {
			filterMap = mergeBeforeAndAfter(before, after);
		} else {
			filterMap = new HashMap<>(after);
		}
		final Document filter = filter(filterMap, currentProperty.getJoinKeys());
		mergeResult.getFilter().putAll(filter);
		switch (operation) {
			case INSERT:
			case UPDATE:
				Map<String, Object> removeFields = mergeBundle.getRemovefields();
				Document setOperateDoc = new Document();
				Document unsetOperateDoc = new Document();
				Map<String, Object> flatValue = new Document();
				MapUtil.recursiveFlatMap(after, flatValue, "");
				after = MapUtils.isNotEmpty(flatValue) ? flatValue : after;
				if (EmptyKit.isNotEmpty(targetPath)) {
					for (Map.Entry<String, Object> entry : after.entrySet()) {
						setOperateDoc.append(targetPath + "." + entry.getKey(), entry.getValue());
					}
				} else {
					setOperateDoc.putAll(after);
				}
				final Document update = mergeResult.getUpdate();
				if (removeFields != null) {
					if (EmptyKit.isNotEmpty(targetPath)) {
						for (Map.Entry<String, Object> entry : removeFields.entrySet()) {
							Map<String, Object> finalAfter = after;
							if (after.keySet().stream().noneMatch(v -> (v.startsWith(entry.getKey() + ".") || entry.getKey().startsWith(v + ".")) && finalAfter.get(v) instanceof ArrayList)) {
								unsetOperateDoc.append(targetPath + "." + entry.getKey(), entry.getValue());
							}
						}
					} else {
						for (Map.Entry<String, Object> entry : removeFields.entrySet()) {
							Map<String, Object> finalAfter = after;
							if (after.keySet().stream().noneMatch(v -> (v.startsWith(entry.getKey() + ".") || entry.getKey().startsWith(v + ".")) && finalAfter.get(v) instanceof ArrayList)) {
								unsetOperateDoc.append(entry.getKey(), entry.getValue());
							}
						}
					}
					if (update.containsKey("$unset")) {
						if (unsetOperateDoc.size() > 0) {
							update.get("$unset", Document.class).putAll(unsetOperateDoc);
						}
					} else {
						if (unsetOperateDoc.size() > 0) {
							update.put("$unset", unsetOperateDoc);
						}
					}
				}
				Document setOperateDocFiltered = filterSetDocByUnsetDoc(setOperateDoc, unsetOperateDoc);
				if (update.containsKey("$set")) {
					update.get("$set", Document.class).putAll(setOperateDocFiltered);
				} else {
					update.put("$set", setOperateDocFiltered);
				}
				mergeResult.getUpdateOptions().upsert(true);
				if (mergeResult.getOperation() == null) {
					mergeResult.setOperation(MergeResult.Operation.UPDATE);
				}
				break;
			case DELETE:
				if (mergeResult.getOperation() == null) {
					mergeResult.setOperation(MergeResult.Operation.DELETE);
				}
				break;
		}
	}

	public static void updateMerge(MergeBundle mergeBundle, MergeTableProperties currentProperty, MergeResult mergeResult, Set<String> sharedJoinKeys, MergeFilter mergeFilter) {
		boolean array = currentProperty.getIsArray();
		MergeBundle.EventOperation operation = mergeBundle.getOperation();
		Map<String, Object> before = mergeBundle.getBefore();
		Map<String, Object> after = mergeBundle.getAfter();
		String targetPath = MergeUtils.dynamicKey(currentProperty.getTargetPath(), after);
		removeIdIfNeed(after, currentProperty);
		Map<String, Object> filterMap = buildFilterMap(operation, after, before);
		Document filter = filter(
				filterMap,
				currentProperty.getJoinKeys()
		);
		mergeResult.getFilter().putAll(filter);
		if (array) {
			List<String> arrayKeys = currentProperty.getArrayKeys();
			filterMap = new HashMap<>(null != after ? after : before);
			if (null != before && CollectionUtils.isNotEmpty(arrayKeys)) {
				for (String arrayKey : arrayKeys) {
					Object value = MapUtil.getValueByKey(before, arrayKey);
					if (null != value) {
						filterMap.put(arrayKey, value);
					}
				}
			}
			final List<Document> arrayFilter = arrayFilter(
					filterMap,
					currentProperty.getJoinKeys(),
					currentProperty.getArrayPath()
			);
			mergeResult.getUpdateOptions().arrayFilters(arrayFilter);
		}
		appendAllParentMergeFilters(mergeResult, mergeFilter);

		Map<String, Object> value = MapUtils.isNotEmpty(after) ? after : before;
		Map<String, Object> removeFields = mergeBundle.getRemovefields();

		String updatePatch = targetPath;
		if (array) {
			if (targetPath.contains(".")) {
				final String targetPathFirst = targetPath.substring(0, targetPath.lastIndexOf("."));
				final String targetPathLast = targetPath.substring(targetPath.lastIndexOf(".") + 1);
				updatePatch = targetPathFirst + ".$[element1]." + targetPathLast;
			} else {
				updatePatch = targetPath + ".$[element1]";
			}
		}

		Document updateOpDoc = new Document();
		Document unsetOpDoc = new Document();
		Map<String, Object> flatValue = new Document();
		MapUtil.recursiveFlatMap(value, flatValue, "");
		value = MapUtils.isNotEmpty(flatValue) ? flatValue : value;
		if (EmptyKit.isNotEmpty(updatePatch)) {
			for (Map.Entry<String, Object> entry : value.entrySet()) {
				updateOpDoc.append(updatePatch + "." + entry.getKey(), entry.getValue());
			}
			if (removeFields != null) {
				for (Map.Entry<String, Object> entry : removeFields.entrySet()) {
					Map<String, Object> finalAfter = value;
					if (value.keySet().stream().noneMatch(v -> (v.startsWith(entry.getKey() + ".") || entry.getKey().startsWith(v + ".")) && finalAfter.get(v) instanceof ArrayList)) {
						unsetOpDoc.append(updatePatch + "." + entry.getKey(), entry.getValue());
					}
				}
			}
		} else {
			updateOpDoc.putAll(value);
			if (removeFields != null) {
				for (Map.Entry<String, Object> entry : removeFields.entrySet()) {
					Map<String, Object> finalAfter = value;
					if (value.keySet().stream().noneMatch(v -> (v.startsWith(entry.getKey() + ".") || entry.getKey().startsWith(v + ".")) && finalAfter.get(v) instanceof ArrayList)) {
						unsetOpDoc.append(entry.getKey(), entry.getValue());
					}
				}
			}
		}
		if (mergeResult.getOperation() == null) {
			mergeResult.setOperation(MergeResult.Operation.UPDATE);
		}
		switch (operation) {
			case INSERT:
			case UPDATE:
				if (MapUtils.isNotEmpty(unsetOpDoc)) {
					removeShareKeys(sharedJoinKeys, unsetOpDoc, array);
					if (mergeResult.getUpdate().containsKey("$unset")) {
						if (!unsetOpDoc.isEmpty()) {
							mergeResult.getUpdate().get("$unset", Document.class).putAll(unsetOpDoc);
						}
					} else {
						if (!unsetOpDoc.isEmpty()) {
							mergeResult.getUpdate().put("$unset", unsetOpDoc);
						}
					}
				}
				Document updateOpDocFiltered = filterSetDocByUnsetDoc(updateOpDoc, unsetOpDoc);
				if (mergeResult.getUpdate().containsKey("$set")) {
					mergeResult.getUpdate().get("$set", Document.class).putAll(updateOpDocFiltered);
				} else {
					mergeResult.getUpdate().put("$set", updateOpDocFiltered);
				}
				break;
			case DELETE:
				removeShareKeys(sharedJoinKeys, updateOpDoc, array);
				if (mergeResult.getUpdate().containsKey("$unset")) {
					mergeResult.getUpdate().get("$unset", Document.class).putAll(updateOpDoc);
				} else {
					mergeResult.getUpdate().put("$unset", updateOpDoc);
				}
				break;
		}
	}

	private static void removeShareKeys(Set<String> sharedJoinKeys, Document update, boolean array) {
		update.keySet().removeIf(key -> {
			if (array) {
				String keyRemoveElement = key.replace(".$[element1]", "");
				return isShareJoinKey(sharedJoinKeys, keyRemoveElement);
			} else {
				return isShareJoinKey(sharedJoinKeys, key);
			}
		});
	}

	private static boolean isShareJoinKey(Set<String> sharedJoinKey, String field) {
		if (null == sharedJoinKey) {
			return false;
		}
		if (StringUtils.isBlank(field)) {
			return false;
		}
		return sharedJoinKey.contains(field);
	}

	public static void updateIntoArrayMerge(MergeBundle mergeBundle, MergeTableProperties currentProperty, MergeResult mergeResult, MergeFilter mergeFilter) {
		String targetPath = currentProperty.getTargetPath();
		boolean array = currentProperty.getIsArray();
		Map<String, Object> before = mergeBundle.getBefore();
		Map<String, Object> after = mergeBundle.getAfter();
		Map<String, Object> removefields = mergeBundle.getRemovefields();
		MergeBundle.EventOperation operation = mergeBundle.getOperation();
		List<String> arrayKeys = currentProperty.getArrayKeys();
		Map<String, Object> filterMap = buildFilterMap(operation, after, before);
		if (array) {
			List<Document> arrayFilter;
			if (operation == MergeBundle.EventOperation.UPDATE) {
				arrayFilter = arrayFilter(
						filterMap,
						currentProperty.getJoinKeys(),
						arrayKeys,
						currentProperty.getArrayPath()
				);
			} else {
				arrayFilter = arrayFilter(
						filterMap,
						currentProperty.getJoinKeys(),
						currentProperty.getArrayPath());
			}
			mergeResult.getUpdateOptions().arrayFilters(arrayFilter);
		} else {
			Document filter = filter(filterMap, currentProperty.getJoinKeys());
			mergeResult.getFilter().putAll(filter);

			if (operation == MergeBundle.EventOperation.UPDATE) {
				List<Document> arrayFilter = arrayFilterForArrayMerge(
						filterMap,
						currentProperty.getArrayKeys(),
						currentProperty.getTargetPath(),
						currentProperty.getArrayPath()
				);
				mergeResult.getUpdateOptions().arrayFilters(arrayFilter);
			}
		}
		appendAllParentMergeFilters(mergeResult, mergeFilter);

		Document updateOpDoc = new Document();
		Document unsetOpDoc = new Document();

		if (mergeResult.getOperation() == null) {
			mergeResult.setOperation(MergeResult.Operation.UPDATE);
		}
		switch (operation) {
			case INSERT:
				// Ensuring Array Idempotence, just for one level array
				Document elemMatchDoc = new Document();
				arrayKeys.forEach(key -> {
					if (!array) {
						elemMatchDoc.append(key, new Document("$eq", after.get(key)));
					}
				});
				if (!elemMatchDoc.isEmpty()) {
					mergeResult.getFilter().append(targetPath, new Document("$not", new Document("$elemMatch", elemMatchDoc)));
				}

				if (array) {
					String[] paths = targetPath.split("\\.");
					if (paths.length > 1) {
						updateOpDoc.append(paths[0] + ".$[element1]." + paths[1], after);
					} else {
						updateOpDoc.append(paths[0] + ".$[element1]", after);
					}
				} else {
					updateOpDoc.append(targetPath, after);
				}
				if (mergeResult.getUpdate().containsKey("$addToSet")) {
					mergeResult.getUpdate().get("$addToSet", Document.class).putAll(updateOpDoc);
				} else {
					mergeResult.getUpdate().put("$addToSet", updateOpDoc);
				}
				break;
			case UPDATE:
				for (Map.Entry<String, Object> entry : after.entrySet()) {
					if (array) {
						String[] paths = targetPath.split("\\.");
						if (paths.length > 1) {
							updateOpDoc.append(paths[0] + ".$[element1]." + paths[1] + ".$[element2]." + entry.getKey(), entry.getValue());
						} else {
							updateOpDoc.append(targetPath + ".$[element1]." + entry.getKey(), entry.getValue());
						}
					} else {
						updateOpDoc.append(targetPath + ".$[element1]." + entry.getKey(), entry.getValue());
					}
				}
				if (mergeResult.getUpdate().containsKey("$set")) {
					mergeResult.getUpdate().get("$set", Document.class).putAll(updateOpDoc);
				} else {
					mergeResult.getUpdate().put("$set", updateOpDoc);
				}
				if (removefields != null) {
					for (String removeField : removefields.keySet()) {
						if (after.keySet().stream().noneMatch(v -> v.startsWith(removeField + ".") || removeField.startsWith(v + "."))) {
							if (array) {
								String[] paths = targetPath.split("\\.");
								if (paths.length > 1) {
									unsetOpDoc.append(paths[0] + ".$[element1]." + paths[1] + ".$[element2]." + removeField, true);
								} else {
									unsetOpDoc.append(targetPath + ".$[element1]." + removeField, true);
								}
							} else {
								unsetOpDoc.append(targetPath + ".$[element1]." + removeField, true);
							}
						}
					}
					if (mergeResult.getUpdate().containsKey("$unset")) {
						if (!unsetOpDoc.isEmpty()) {
							mergeResult.getUpdate().get("$unset", Document.class).putAll(unsetOpDoc);
						}
					} else {
						if (!unsetOpDoc.isEmpty()) {
							mergeResult.getUpdate().put("$unset", unsetOpDoc);
						}
					}
				}
				break;
			case DELETE:
				updateOpDoc = buildPullDocument(mergeBundle.getBefore(), arrayKeys, array, targetPath);
				if (mergeResult.getUpdate().containsKey("$pull")) {
					mergeResult.getUpdate().get("$pull", Document.class).putAll(updateOpDoc);
				} else {
					mergeResult.getUpdate().put("$pull", updateOpDoc);
				}
				break;
		}
	}

	private static Map<String, Object> buildFilterMap(MergeBundle.EventOperation operation, Map<String, Object> after, Map<String, Object> before) {
		Map<String, Object> filterMap;
		if (MergeBundle.EventOperation.INSERT == operation || MergeBundle.EventOperation.UPDATE == operation) {
			filterMap = new HashMap<>(after);
		} else {
			filterMap = new HashMap<>(before);
		}
		return filterMap;
	}

	protected static Document buildUnsetDocument(Set<String> sharedJoinKeys, Map<String, Object> data, String targetPath, boolean isArray, boolean firstMergeResult) {
		Document unsetDoc = new Document();
		if (isArray) {
			if (firstMergeResult && StringUtils.isNotBlank(targetPath)) {
				data.keySet().forEach(key -> {
					if (haveDot(key)) {
						return;
					}
					if (sharedJoinKeys.contains(String.join(".", targetPath, key))) {
						return;
					}
					unsetDoc.append(String.join(".", targetPath, "$[element1]", key), true);
				});
			}
		} else {
			data.keySet().forEach(key -> {
				if (haveDot(key)) {
					return;
				}
				String unsetKey = key;
				if (EmptyKit.isNotEmpty(targetPath)) {
					unsetKey = String.join(".", targetPath, key);
				}
				unsetDoc.append(unsetKey, true);
			});
			unsetDoc.keySet().removeIf(key -> isShareJoinKey(sharedJoinKeys, key));
		}
		return unsetDoc;
	}

	private static boolean haveDot(String str) {
		if (StringUtils.isBlank(str)) {
			return false;
		}
		return str.contains(".");
	}

	private static Document buildPullDocument(Map<String, Object> data, List<String> arrayKeys, boolean array, String targetPath) {
		Document updateOpDoc = new Document();
		for (String arrayKey : arrayKeys) {
			Object value = MapUtil.getValueByKey(data, arrayKey);
			updateOpDoc.append(arrayKey, value);
		}
		if (array) {
			String[] paths = targetPath.split("\\.");
			if (paths.length == 2) {
				updateOpDoc = new Document(paths[0] + ".$[element1]." + paths[1], updateOpDoc);
			}
		} else {
			updateOpDoc = new Document(targetPath, updateOpDoc);
		}
		return updateOpDoc;
	}

	protected static MergeBundle mergeBundle(TapRecordEvent tapRecordEvent) {
		Map<String, Object> before = null;
		Map<String, Object> after = null;
		MergeBundle.EventOperation eventOperation;
		Map<String, Object> removeFieldsMap = removeFieldsWrapper(tapRecordEvent);
		if (tapRecordEvent instanceof TapInsertRecordEvent) {
			after = ((TapInsertRecordEvent) tapRecordEvent).getAfter();
			eventOperation = MergeBundle.EventOperation.INSERT;
		} else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
			before = ((TapUpdateRecordEvent) tapRecordEvent).getBefore();
			after = ((TapUpdateRecordEvent) tapRecordEvent).getAfter();
			eventOperation = MergeBundle.EventOperation.UPDATE;
		} else {
			before = ((TapDeleteRecordEvent) tapRecordEvent).getBefore();
			eventOperation = MergeBundle.EventOperation.DELETE;
		}

		return new MergeBundle(eventOperation, before, after, removeFieldsMap);
	}

	private static Map<String, Object> removeFieldsWrapper(TapRecordEvent tapRecordEvent) {
		List<String> removedFields = null;
		Map<String, Object> removefieldsMap = new HashMap<>();
		if (tapRecordEvent instanceof TapInsertRecordEvent) {
			removedFields = ((TapInsertRecordEvent) tapRecordEvent).getRemovedFields();
		} else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
			removedFields = ((TapUpdateRecordEvent) tapRecordEvent).getRemovedFields();
		}
		if (null == removedFields) {
			return new HashMap<>();
		}
		removedFields.forEach(removeField -> removefieldsMap.put(removeField, true));
		return removefieldsMap;
	}

	private static Document filter(Map<String, Object> data, List<Map<String, String>> joinKeys) {
		Document document = new Document();
		for (Map<String, String> joinKey : joinKeys) {
			String target = joinKey.get("target");
			target = MergeUtils.dynamicKey(target, data);
			document.put(target, MapUtil.getValueByKey(data, joinKey.get("source")));
		}
		return document;
	}

	protected static Document unsetFilter(Map<String, Object> before, Map<String, Object> after, List<Map<String, String>> joinKeys, int topLevel) {
		Document document = new Document();
		for (Map<String, String> joinKey : joinKeys) {
			String key = joinKey.get("target");
			Object value;
			if (topLevel == 1) {
				value = MapUtil.getValueByKey(after, key);
			} else {
				value = MapUtil.getValueByKey(before, key);
			}
			document.put(key, value);
		}
		return document;
	}

	private static List<Document> arrayFilter(Map<String, Object> data, List<Map<String, String>> joinKeys, String arrayPath) {
		List<Document> arrayFilter = new ArrayList<>();
		Document filter = new Document();
		for (Map<String, String> joinKey : joinKeys) {
			filter.put("element1." + getArrayMatchString(arrayPath, joinKey), MapUtil.getValueByKey(data, joinKey.get("source")));
		}
		arrayFilter.add(filter);
		return arrayFilter;
	}

	private static String getArrayMatchString(String arrayPath, Map<String, String> joinKey) {
		String targetStr = joinKey.get("target");
		if (targetStr.startsWith(arrayPath)) {
			targetStr = targetStr.substring(arrayPath.length() + 1);
		}
		return targetStr;
	}

	private static List<Document> arrayFilterForArrayMerge(Map<String, Object> data, List<String> arrayKeys, String targetPath, String arrayPath) {
		List<Document> arrayFilter = new ArrayList<>();
		Document filter = new Document();
		for (String arrayKey : arrayKeys) {
			String[] paths = arrayKey.split("\\.");
			filter.put("element1." + paths[paths.length - 1], MapUtil.getValueByKey(data, arrayKey));
		}
		arrayFilter.add(filter);
		return arrayFilter;
	}

	private static List<Document> arrayFilter(Map<String, Object> data, List<Map<String, String>> joinKeys, List<String> arrayKeys, String arrayPath) {
		List<Document> arrayFilter = new ArrayList<>();
		for (Map<String, String> joinKey : joinKeys) {
			Document filter = new Document();
			filter.put("element1." + getArrayMatchString(arrayPath, joinKey)/*paths[paths.length - 1]*/, MapUtil.getValueByKey(data, joinKey.get("source")));
			arrayFilter.add(filter);
		}

		for (String arrayKey : arrayKeys) {
			Document filter = new Document();
			String[] paths = arrayKey.split("\\.");
			filter.put("element2." + paths[paths.length - 1], MapUtil.getValueByKey(data, arrayKey));
			arrayFilter.add(filter);
		}
		return arrayFilter;
	}

	protected static void appendAllParentMergeFilters(MergeResult mergeResult, MergeFilter mergeFilter) {
		if (null == mergeResult || MapUtils.isEmpty(mergeResult.getFilter()) || null == mergeFilter) {
			return;
		}
		Document parentFilters = mergeFilter.appendFilters();
		if (null == parentFilters) {
			return;
		}
		Document filter = mergeResult.getFilter();
		for (Map.Entry<String, Object> entry : parentFilters.entrySet()) {
			if (MergeFilterManager.test(entry)) {
				continue;
			}
			if (!filter.containsKey(entry.getKey())) {
				filter.put(entry.getKey(), entry.getValue());
			}
		}
	}

	protected static Document filterSetDocByUnsetDoc(Document setDoc, Document unsetDoc) {
		if (MapUtils.isEmpty(setDoc) || MapUtils.isEmpty(unsetDoc)) {
			return setDoc;
		}
		for (String key : unsetDoc.keySet()) {
			setDoc.remove(key);
			String[] split = key.split("\\.");
			if (split.length > 1) {
				String parentKey = null;
				for (String s : split) {
					if (StringUtils.isBlank(parentKey)) {
						parentKey = s;
					} else {
						parentKey = parentKey + "." + s;
					}
					Object obj = setDoc.get(parentKey);
					if (obj instanceof Map && ((Map<?, ?>) obj).isEmpty()) {
						setDoc.remove(parentKey);
					}
				}
			}
		}
		return setDoc;
	}

	protected static void removeIdIfNeed(Map<String, Object> data, MergeTableProperties mergeTableProperties) {
		if (MapUtils.isEmpty(data) || null == mergeTableProperties) {
			return;
		}
		MergeTableProperties.MergeType mergeType = mergeTableProperties.getMergeType();
		if (mergeType != MergeTableProperties.MergeType.updateWrite) {
			return;
		}
		String targetPath = mergeTableProperties.getTargetPath();
		if (StringUtils.isNotBlank(targetPath)) {
			return;
		}
		List<Map<String, String>> joinKeys = mergeTableProperties.getJoinKeys();
		for (Map<String, String> joinKey : joinKeys) {
			if (joinKey.containsValue("_id")) {
				return;
			}
		}
		data.remove("_id");
	}

	public static Map<String, Object> mergeBeforeAndAfter(Map<String, Object> before, Map<String, Object> after) {
		Map<String, Object> result = new HashMap<>();
		if (null != before) {
			result.putAll(before);
			if (null != after) {
				after.forEach((k, v) -> {
					if (!result.containsKey(k)) {
						result.put(k, v);
					}
				});
			}
		} else {
			result.putAll(after);
		}
		return result;
	}
}
