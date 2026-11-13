package io.tapdata.mongodb.writer;

import com.mongodb.client.model.*;
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
import io.tapdata.mongodb.util.MapDiffUtil;
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
			List<MapDiffUtil.KeyDiffInfo> updateKeyDiffInfos = null;
			if (tapRecordEvent instanceof TapInsertRecordEvent) {
				inserted.incrementAndGet();
			} else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
				if (MapUtils.isNotEmpty(((TapUpdateRecordEvent) tapRecordEvent).getBefore())
						&& MapUtils.isNotEmpty(((TapUpdateRecordEvent) tapRecordEvent).getAfter())) {
					updateKeyDiffInfos = MapDiffUtil.compareAndFindMissingKeys(((TapUpdateRecordEvent) tapRecordEvent).getBefore(),
							((TapUpdateRecordEvent) tapRecordEvent).getAfter());
				}
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
						level,
						updateKeyDiffInfos
				);

				if (CollectionUtils.isNotEmpty(mergeResults)) {
					for (MergeResult mergeResult : mergeResults) {
						final MergeResult.Operation operation = mergeResult.getOperation();
						if (null == operation) {
							continue;
						}
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
			int topLevel,
			List<MapDiffUtil.KeyDiffInfo> updateKeyDiffInfos
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
				1,
				updateKeyDiffInfos,
				extractMainTableKeys(mergeBundle, topLevel)
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
			int loopTime,
			List<MapDiffUtil.KeyDiffInfo> updateKeyDiffInfos
	) {
		recursiveMerge(mergeBundle, properties, mergeResults, mergeLookupResults, mergeResult, unsetResult,
				updateJoinKeys, sharedJoinKeys, parentProperties, mergeFilter, topLevel, loopTime,
				updateKeyDiffInfos, extractMainTableKeys(mergeBundle, topLevel));
	}

	private static Set<String> extractMainTableKeys(MergeBundle mergeBundle, int topLevel) {
		if (topLevel != 1 || mergeBundle == null) {
			return Collections.emptySet();
		}
		Map<String, Object> after = mergeBundle.getAfter();
		Map<String, Object> source = MapUtils.isNotEmpty(after) ? after : mergeBundle.getBefore();
		if (MapUtils.isEmpty(source)) {
			return Collections.emptySet();
		}
		return new HashSet<>(source.keySet());
	}

	private static void recursiveMerge(
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
			int loopTime,
			List<MapDiffUtil.KeyDiffInfo> updateKeyDiffInfos,
			Set<String> mainTableKeys
	) {
		boolean unsetResultNull = null == unsetResult;
		MergeResult updateKeyDiff = null;
		switch (properties.getMergeType()) {
			case updateOrInsert:
				upsertMerge(mergeBundle, properties, mergeResult);
				if (!mergeFilter.isAppend() && mergeFilter.getFilters().isEmpty()) {
					mergeFilter.addFilter(filter(mergeBundle.getAfter(), properties.getJoinKeys()));
				}
				break;
			case updateWrite:
				unsetResult = updateWriteUnsetMerge(mergeBundle, properties, updateJoinKeys, unsetResult, sharedJoinKeys, mergeFilter, topLevel, loopTime, mainTableKeys);
				if (unsetResultNull) {
					addUnsetMerge(mergeResults, unsetResult);
				}
				mergeResult = addMergeResults(mergeResults, mergeResult);
				mergeResult = updateMergeIfNeed(mergeBundle, properties, mergeResult, sharedJoinKeys, mergeFilter);
				if (MergeBundle.EventOperation.DELETE == mergeBundle.getOperation()) {
					mergeResult = appendDescendantUnsetOnParentDelete(mergeBundle, properties, mergeResult, sharedJoinKeys, mergeFilter);
				}
				updateKeyDiff = keyDiffUnsetWhenUpdateWrite(mergeResult, updateKeyDiffInfos);
				break;
			case updateIntoArray:
				if (MergeBundle.EventOperation.DELETE == mergeBundle.getOperation() && mergeBundle.isRecursive()) {
					// 父 DELETE 级联清理必须在 updateIntoArrayUnsetMerge 之前判断：该方法在父级为 updateWrite/updateIntoArray 时
					// 会把 operation 改成 INSERT，导致子孙数组既不 $pull 也不 $unset
					mergeResult = addMergeResults(mergeResults, mergeResult);
					if (isNestedInParentArrayElement(properties, parentProperties)) {
						// 嵌套在父数组元素内（如 parts.attrList）：父级已 $pull/$unset 整个元素，子孙随之移除；
						// 再按子表关联值 arrayFilters 去 $pull 会误删同一主文档中其他关联值相同的父元素下的数组元素
						break;
					}
					mergeResult = appendArrayDeleteOnParentDelete(mergeBundle, properties, mergeResult, sharedJoinKeys, mergeFilter);
					break;
				}
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
			// 以当前节点的事件类型决定子孙处理方式；循环中 mergeBundle 会被替换为子 bundle，不能用上一个子节点被改写后的 operation 判断兄弟节点
			final boolean parentDelete = MergeBundle.EventOperation.DELETE == mergeBundle.getOperation();
			for (MergeLookupResult mergeLookupResult : mergeLookupResults) {
				privateLoopTime++;
				final Map<String, Object> data = mergeLookupResult.getData();
				if (parentDelete) {
					// 父 DELETE 级联清理：不得把子孙 lookup 数据改写成 INSERT/$set
					// 子孙 $unset/$pull 必须打在父文档上，因此无论 MergeFilter 是否 append，都把父 filter 传下去
					if (null != mergeFilter && null != mergeResult && MapUtils.isNotEmpty(mergeResult.getFilter())) {
						mergeFilter.addFilter(mergeResult.getFilter());
					}
					mergeBundle = new MergeBundle(MergeBundle.EventOperation.DELETE, data, null);
					mergeBundle.setRecursive(true);
					mergeBundle.setDataExists(false);
				} else {
					mergeBundle = new MergeBundle(MergeBundle.EventOperation.INSERT, null, data);
					mergeBundle.setRecursive(true);
					mergeBundle.setDataExists(mergeLookupResult.isDataExists());
					if (mergeFilter.isAppend() && null != mergeResult) {
						mergeFilter.addFilter(mergeResult.getFilter());
					}
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
						privateLoopTime,
						null,
						mainTableKeys
				);
				recursiveOnce = true;
			}
			return;
		}

		// 没有 operation 的结果不产生写入；写入端按 operation 生成 WriteModel，放进来会 NPE（如父 DELETE 级联中跳过/无法定位父文档的子孙）
		Optional.ofNullable(mergeResult).filter(result -> null != result.getOperation()).ifPresent(mergeResults::add);
		Optional.ofNullable(updateKeyDiff).ifPresent(mergeResults::add);
		if (mergeFilter.isAppend()) {
			mergeFilter.removeLast();
		}
	}

	private static MergeResult keyDiffUnsetWhenUpdateWrite(MergeResult mergeResult, List<MapDiffUtil.KeyDiffInfo> updateKeyDiffInfos) {
		if (CollectionUtils.isNotEmpty(updateKeyDiffInfos)) {
			Document unset = new Document();
			updateKeyDiffInfos.forEach(updateKeyDiffInfo -> unset.append(updateKeyDiffInfo.getFullPath(), 1));
			MergeResult updateKeyDiffUnset = new MergeResult();
			updateKeyDiffUnset.setFilter(mergeResult.getFilter());
			updateKeyDiffUnset.setOperation(MergeResult.Operation.UPDATE);
			updateKeyDiffUnset.setUpdate(new Document("$unset", unset));
			updateKeyDiffUnset.setUpdateOptions(new UpdateOptions().upsert(false));
			return updateKeyDiffUnset;
		}
		return null;
	}


	private static MergeResult appendDescendantUnsetOnParentDelete(MergeBundle mergeBundle, MergeTableProperties properties,
			MergeResult mergeResult, Set<String> sharedJoinKeys, MergeFilter mergeFilter) {
		Map<String, Object> value = MapUtils.isNotEmpty(mergeBundle.getAfter()) ? mergeBundle.getAfter() : mergeBundle.getBefore();
		String targetPath = MergeUtils.dynamicKey(properties.getTargetPath(), value);
		if (StringUtils.isBlank(targetPath) || isShareJoinKey(sharedJoinKeys, targetPath)) {
			return mergeResult;
		}
		if (null == mergeResult) {
			mergeResult = new MergeResult();
		}
		// 父文档定位必须来自父级 filter，不能用子孙自己的 joinKeys（否则会写成 {idC: ...} 而打不到主文档）
		if (MapUtils.isEmpty(mergeResult.getFilter()) && null != mergeFilter) {
			Document parentFilters = mergeFilter.appendFilters();
			if (MapUtils.isNotEmpty(parentFilters)) {
				mergeResult.getFilter().putAll(parentFilters);
			}
		} else {
			appendAllParentMergeFilters(mergeResult, mergeFilter);
		}
		if (MapUtils.isEmpty(mergeResult.getFilter())) {
			return mergeResult;
		}
		if (null == mergeResult.getOperation()) {
			mergeResult.setOperation(MergeResult.Operation.UPDATE);
		}
		Document unsetDoc = new Document(targetPath, true);
		if (mergeResult.getUpdate().containsKey("$unset")) {
			mergeResult.getUpdate().get("$unset", Document.class).putAll(unsetDoc);
		} else {
			mergeResult.getUpdate().put("$unset", unsetDoc);
		}
		return mergeResult;
	}

	private static boolean isNestedInParentArrayElement(MergeTableProperties properties, MergeTableProperties parentProperties) {
		return null != properties && properties.getIsArray()
				&& null != parentProperties && MergeTableProperties.MergeType.updateIntoArray == parentProperties.getMergeType();
	}

	private static MergeResult appendArrayDeleteOnParentDelete(MergeBundle mergeBundle, MergeTableProperties properties,
			MergeResult mergeResult, Set<String> sharedJoinKeys, MergeFilter mergeFilter) {
		List<String> arrayKeys = properties.getArrayKeys();
		Map<String, Object> before = mergeBundle.getBefore();
		boolean canPull = CollectionUtils.isNotEmpty(arrayKeys) && MapUtils.isNotEmpty(before)
				&& arrayKeys.stream().anyMatch(key -> MapUtil.getValueByKey(before, key) != null);
		if (canPull) {
			if (null == mergeResult) {
				mergeResult = new MergeResult();
			}
			if (properties.getIsArray()) {
				// 嵌套在父数组元素内的子孙（如 parts.attrList）：updateIntoArrayMerge 只生成 arrayFilters 不带主文档条件，
				// 必须用父级 filter 限定主文档，否则 q:{} 会按 arrayFilters 跨文档误删其他主文档中同关联值的数组元素
				Document parentFilters = null == mergeFilter ? null : mergeFilter.appendFilters();
				if (MapUtils.isEmpty(parentFilters)) {
					return mergeResult;
				}
				updateIntoArrayMerge(mergeBundle, properties, mergeResult, mergeFilter);
				for (Map.Entry<String, Object> entry : parentFilters.entrySet()) {
					if (!MergeFilterManager.test(entry)) {
						mergeResult.getFilter().putIfAbsent(entry.getKey(), entry.getValue());
					}
				}
				return mergeResult;
			}
			return appendArrayPullOnParentDelete(mergeBundle, properties, mergeResult, mergeFilter);
		}
		return appendDescendantUnsetOnParentDelete(mergeBundle, properties, mergeResult, sharedJoinKeys, mergeFilter);
	}

	private static MergeResult appendArrayPullOnParentDelete(MergeBundle mergeBundle, MergeTableProperties properties,
			MergeResult mergeResult, MergeFilter mergeFilter) {
		// 父 DELETE 时父行字段已在同批前序写入中被 $unset，不能再用子表 joinKeys 映射到父字段的条件定位主文档，
		// 否则按序执行后 $pull 匹配 0 条；只用父级 filter 定位主文档，按 arrayKeys 拉取元素
		if (MapUtils.isEmpty(mergeResult.getFilter()) && null != mergeFilter) {
			Document parentFilters = mergeFilter.appendFilters();
			if (MapUtils.isNotEmpty(parentFilters)) {
				mergeResult.getFilter().putAll(parentFilters);
			}
		} else {
			appendAllParentMergeFilters(mergeResult, mergeFilter);
		}
		if (MapUtils.isEmpty(mergeResult.getFilter())) {
			return mergeResult;
		}
		if (null == mergeResult.getOperation()) {
			mergeResult.setOperation(MergeResult.Operation.UPDATE);
		}
		Document pullDoc = buildPullDocument(mergeBundle.getBefore(), properties.getArrayKeys(), false, properties.getTargetPath());
		if (mergeResult.getUpdate().containsKey("$pull")) {
			mergeResult.getUpdate().get("$pull", Document.class).putAll(pullDoc);
		} else {
			mergeResult.getUpdate().put("$pull", pullDoc);
		}
		return mergeResult;
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
			MergeResult mergeResult, Set<String> sharedJoinKeys, MergeFilter mergeFilter, int topLevel,
			int loopTime, Set<String> mainTableKeys) {
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
			Map<String, Object> updateJoinKeyParentBefore = updateJoinKey.getParentBefore();
			List<Map<String, String>> joinKeys = currentProperty.getJoinKeys();
			Document filter;
			mergeResult = new MergeResult();
			filter = unsetFilterForUpdateJoinKey(updateJoinKeyBefore, updateJoinKeyAfter, joinKeys, topLevel, loopTime,updateJoinKeyParentBefore);
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
		Set<String> effectiveSharedJoinKeys = sharedJoinKeys;
		if (EmptyKit.isEmpty(targetPath) && CollectionUtils.isNotEmpty(mainTableKeys)) {
			effectiveSharedJoinKeys = new HashSet<>();
			if (sharedJoinKeys != null) {
				effectiveSharedJoinKeys.addAll(sharedJoinKeys);
			}
			effectiveSharedJoinKeys.addAll(mainTableKeys);
		}
		Document unsetDoc = buildUnsetDocument(effectiveSharedJoinKeys, after, targetPath, isArray, firstMergeResult);
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
			if (StringUtils.isBlank(targetPath)) {
				return null;
			}
			MergeInfo.UpdateJoinKey updateJoinKey = null == updateJoinKeys ? null : updateJoinKeys.get(id);
			if (null == updateJoinKey || MapUtils.isEmpty(updateJoinKey.getBefore())) {
				return mergeResult;
			}
			Document pullCondition = buildJoinKeyPullCondition(currentProperty.getJoinKeys(), updateJoinKey.getBefore());
			if (pullCondition.isEmpty()) {
				return mergeResult;
			}
			boolean newResult = (null == mergeResult);
			if (newResult) {
				mergeResult = new MergeResult();
			}
			Document parentFilters = null != mergeFilter ? mergeFilter.appendFilters() : null;
			if (null != parentFilters) {
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
			if (MapUtils.isEmpty(mergeResult.getFilter())) {
				return newResult ? null : mergeResult;
			}
			appendPullToUpdate(mergeResult.getUpdate(), targetPath, pullCondition);
			if (null == mergeResult.getOperation()) {
				mergeResult.setOperation(MergeResult.Operation.UPDATE);
			}
			return mergeResult;
		}
		MergeInfo.UpdateJoinKey updateJoinKey = null == updateJoinKeys ? null : updateJoinKeys.get(id);
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
		if (StringUtils.isNotBlank(targetPath)) {
			if (!array) {
				Document pullCondition;
				if (CollectionUtils.isNotEmpty(arrayKeys)) {
					pullCondition = buildArrayKeysPullCondition(arrayKeys, mergeBundle.getBefore());
					if (pullCondition.isEmpty()) {
						pullCondition = buildJoinKeyPullCondition(currentProperty.getJoinKeys(), updateJoinKeyBefore);
					}
				} else {
					pullCondition = buildJoinKeyPullCondition(currentProperty.getJoinKeys(), updateJoinKeyBefore);
				}
				if (!pullCondition.isEmpty()) {
					appendPullToUpdate(mergeResult.getUpdate(), targetPath, pullCondition);
				}
			} else {
				Document updateOpDoc = new Document(targetPath, new ArrayList<>());
				if (mergeResult.getUpdate().containsKey("$set")) {
					mergeResult.getUpdate().get("$set", Document.class).putAll(updateOpDoc);
				} else {
					mergeResult.getUpdate().put("$set", updateOpDoc);
				}
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
					null != currentProperty.getArrayPath() ? currentProperty.getArrayPath() : currentProperty.getTargetPath(
					));
			mergeResult.getUpdateOptions().arrayFilters(arrayFilter);
		}
		appendAllParentMergeFilters(mergeResult, mergeFilter);

		Map<String, Object> value = MapUtils.isNotEmpty(after) ? after : before;
		removeIdIfNeed(value, currentProperty);
		Map<String, Object> removeFields = mergeBundle.getRemovefields();

		String targetPath = MergeUtils.dynamicKey(currentProperty.getTargetPath(), value);
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
						overrideArrayKeysFromBefore(filterMap, before, arrayKeys),
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
						overrideArrayKeysFromBefore(filterMap, before, arrayKeys),
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
					unsetDoc.append(unsetKey, true);
				}else if(!unsetKey.equals("_id")){
					unsetDoc.append(unsetKey, true);
				}
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
		return unsetFilter(before, after, joinKeys, topLevel == 1);
	}

	protected static Document unsetFilterForUpdateJoinKey(Map<String, Object> before, Map<String, Object> after,
														  List<Map<String, String>> joinKeys, int topLevel, int loopTime,Map<String,Object> updateJoinKeyParentBefore){
		return unsetFilter(before, after, joinKeys, shouldUseAfterForUpdateJoinKeyUnsetFilter(topLevel, loopTime,updateJoinKeyParentBefore));
	}

	private static Document unsetFilter(Map<String, Object> before, Map<String, Object> after, List<Map<String, String>> joinKeys, boolean useAfter) {
		Document document = new Document();
		for (Map<String, String> joinKey : joinKeys) {
			String key = joinKey.get("target");
			Map<String, Object> filter = useAfter ? after : before;
			if(MapUtil.containsKey(filter, key)){
				Object value = MapUtil.getValueByKey(filter, key);
				document.put(key, value);
			}
		}
		return document;
	}

	private static boolean shouldUseAfterForUpdateJoinKeyUnsetFilter(int topLevel, int loopTime,Map<String,Object> updateJoinKeyParentBefore) {
		return topLevel == 1 && (loopTime > 1 ||  MapUtils.isNotEmpty(updateJoinKeyParentBefore));
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

	protected static Document buildJoinKeyPullCondition(List<Map<String, String>> joinKeys, Map<String, Object> beforeJoinKey) {
		Document pullCondition = new Document();
		if (CollectionUtils.isEmpty(joinKeys) || MapUtils.isEmpty(beforeJoinKey)) {
			return pullCondition;
		}
		for (Map<String, String> joinKey : joinKeys) {
			String sourceField = joinKey.get("source");
			String targetField = joinKey.get("target");
			if (StringUtils.isBlank(sourceField) || StringUtils.isBlank(targetField)) {
				continue;
			}
			Object oldValue = MapUtil.getValueByKey(beforeJoinKey, sourceField);
			if (oldValue == null) {
				oldValue = MapUtil.getValueByKey(beforeJoinKey, targetField);
			}
			if (oldValue == null) {
				continue;
			}
			pullCondition.put(sourceField, oldValue);
		}
		return pullCondition;
	}

	protected static Map<String, Object> overrideArrayKeysFromBefore(Map<String, Object> base, Map<String, Object> before, List<String> arrayKeys) {
		if (CollectionUtils.isEmpty(arrayKeys) || MapUtils.isEmpty(before)) {
			return base;
		}
		Map<String, Object> result = null;
		for (String arrayKey : arrayKeys) {
			if (StringUtils.isBlank(arrayKey)) {
				continue;
			}
			if (!MapUtil.containsKey(before, arrayKey)) {
				continue;
			}
			Object oldValue = MapUtil.getValueByKey(before, arrayKey);
			Object currentValue = null == base ? null : MapUtil.getValueByKey(base, arrayKey);
			if (Objects.equals(oldValue, currentValue)) {
				continue;
			}
			if (null == result) {
				result = null == base ? new HashMap<>() : new HashMap<>(base);
			}
			result.put(arrayKey, oldValue);
		}
		return null == result ? base : result;
	}

	protected static Document buildArrayKeysPullCondition(List<String> arrayKeys, Map<String, Object> beforeImage) {
		Document pullCondition = new Document();
		if (CollectionUtils.isEmpty(arrayKeys) || MapUtils.isEmpty(beforeImage)) {
			return pullCondition;
		}
		for (String arrayKey : arrayKeys) {
			if (StringUtils.isBlank(arrayKey)) {
				continue;
			}
			Object value = MapUtil.getValueByKey(beforeImage, arrayKey);
			if (value == null) {
				continue;
			}
			pullCondition.put(arrayKey, value);
		}
		return pullCondition;
	}

	protected static void appendPullToUpdate(Document update, String targetPath, Document pullCondition) {
		Document pullDoc;
		if (update.containsKey("$pull")) {
			pullDoc = update.get("$pull", Document.class);
		} else {
			pullDoc = new Document();
			update.put("$pull", pullDoc);
		}
		Object existing = pullDoc.get(targetPath);
		if (existing instanceof Document) {
			((Document) existing).putAll(pullCondition);
		} else {
			pullDoc.put(targetPath, pullCondition);
		}
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
