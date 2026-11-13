package io.tapdata.mongodb.writer;

import com.mongodb.client.model.UpdateOptions;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.mongodb.entity.MergeBundle;
import io.tapdata.mongodb.entity.MergeFilter;
import io.tapdata.mongodb.entity.MergeResult;
import io.tapdata.mongodb.merge.MergeFilterManager;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.pdk.apis.entity.merge.MergeLookupResult;
import io.tapdata.pdk.apis.entity.merge.MergeTableProperties;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.time.Instant;
import java.util.*;

import static io.tapdata.common.utils.MergeUtils.dynamicKey;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;

/**
 * @author samuel
 * @Description
 * @create 2024-04-23 15:36
 **/
@DisplayName("Class MongodbMergeOperate Test")
class MongodbMergeOperateTest {

	@Nested
	@DisplayName("Method appendAllParentMergeFilters Test")
	class appendAllParentMergeFiltersTest {
		@Test
		@DisplayName("Test main process")
		void testMainProcess() {
			MergeResult mergeResult = new MergeResult();
			mergeResult.setFilter(new Document("id", 1));
			MergeFilter mergeFilter = new MergeFilter(true);
			mergeFilter.addFilter(new Document("id1", 2));
			MongodbMergeOperate.appendAllParentMergeFilters(mergeResult, mergeFilter);

			assertEquals(2, mergeResult.getFilter().size());
			assertEquals(1, mergeResult.getFilter().getInteger("id"));
			assertEquals(2, mergeResult.getFilter().getInteger("id1"));
		}

		@Test
		@DisplayName("Test input merge result is null")
		void testInputMergeResultIsNull() {
			assertDoesNotThrow(() -> MongodbMergeOperate.appendAllParentMergeFilters(null, new MergeFilter(true)));
			assertDoesNotThrow(() -> MongodbMergeOperate.appendAllParentMergeFilters(new MergeResult(), new MergeFilter(true)));
		}

		@Test
		@DisplayName("Test input merge filter is null")
		void testInputMergeFilterIsNull() {
			assertDoesNotThrow(() -> MongodbMergeOperate.appendAllParentMergeFilters(new MergeResult(), null));
			MergeResult mergeResult = new MergeResult();
			mergeResult.setFilter(new Document("id", 1));
			assertDoesNotThrow(() -> MongodbMergeOperate.appendAllParentMergeFilters(mergeResult, new MergeFilter(true)));
			assertEquals(1, mergeResult.getFilter().size());
			assertEquals(1, mergeResult.getFilter().getInteger("id"));
		}

		@Test
		@DisplayName("Test filter predicate works")
		void testFilterPredicate() {
			MergeResult mergeResult = new MergeResult();
			mergeResult.setFilter(new Document("id", 1));
			MergeFilter mergeFilter = new MergeFilter(true);
			mergeFilter.addFilter(new Document("id1", 2));
			mergeFilter.addFilter(new Document("id2", 3));

			try (
					MockedStatic<MergeFilterManager> mergeFilterManagerMockedStatic = mockStatic(MergeFilterManager.class)
			) {
				mergeFilterManagerMockedStatic.when(() -> MergeFilterManager.test(any(Map.Entry.class))).thenAnswer(invocationOnMock -> {
					Object argument1 = invocationOnMock.getArgument(0);
					Map.Entry<String, Object> entry = (Map.Entry<String, Object>) argument1;
					String key = entry.getKey();
					if ("id1".equals(key)) {
						return true;
					}
					return false;
				});
				MongodbMergeOperate.appendAllParentMergeFilters(mergeResult, mergeFilter);

				assertEquals(2, mergeResult.getFilter().size());
				assertEquals(1, mergeResult.getFilter().getInteger("id"));
				assertEquals(3, mergeResult.getFilter().getInteger("id2"));
			}
		}
	}

	@Nested
	@DisplayName("Method upsertMerge Test")
	class upsertMergeTest {
		@Test
		@DisplayName("test upsert merge, op: u, before: empty, after: {id: 1, _str: 'test1'}, expect filter: {id: 1}")
		void testFilter() {
			Map<String, Object> before = new HashMap<>();
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("_str", "test1");
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.UPDATE, before, after);
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			Map<String, String> joinKey = new HashMap<>();
			joinKey.put("source", "id");
			joinKey.put("target", "id");
			List<Map<String, String>> joinKeys = new ArrayList<>();
			joinKeys.add(joinKey);
			mergeTableProperties.setJoinKeys(joinKeys);
			MergeResult mergeResult = new MergeResult();
			MongodbMergeOperate.upsertMerge(mergeBundle, mergeTableProperties, mergeResult);
			Document filter = mergeResult.getFilter();
			assertEquals(1, filter.getInteger("id"));
		}
	}

	@Nested
	@DisplayName("Method recursiveMerge test")
	class recursiveMergeTest {
		@Test
		@DisplayName("test multiple update write lookup results, expect 3 merge results")
		void test1() {
			Map<String, Object> before1 = new HashMap<>();
			before1.put("id", 1);
			before1.put("col1", "test");
			Map<String, Object> after1 = new HashMap<>();
			after1.put("id", 1);
			after1.put("col1", "test1");
			MergeTableProperties mergeTableProperties1 = new MergeTableProperties();
			mergeTableProperties1.setJoinKeys(new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id");
					put("target", "id");
				}});
			}});
			mergeTableProperties1.setMergeType(MergeTableProperties.MergeType.updateOrInsert);
			MergeBundle mergeBundle1 = new MergeBundle(MergeBundle.EventOperation.UPDATE, before1, after1);
			List<MergeResult> mergeResults = new ArrayList<>();
			List<MergeLookupResult> mergeLookupResults = new ArrayList<>();
			MergeLookupResult mergeLookupResult1 = new MergeLookupResult();
			mergeLookupResult1.setData(new HashMap<String, Object>() {{
				put("id1", 11);
				put("col1", "test");
			}});
			mergeLookupResult1.setDataExists(true);
			mergeLookupResult1.setTapTable(new TapTable("sub1"));
			MergeTableProperties mergeTableProperties2 = new MergeTableProperties();
			mergeTableProperties2.setMergeType(MergeTableProperties.MergeType.updateWrite);
			mergeTableProperties2.setJoinKeys(new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id1");
					put("target", "id1");
				}});
			}});
			mergeTableProperties2.setTargetPath("sub1");
			mergeLookupResult1.setProperty(mergeTableProperties2);
			mergeLookupResults.add(mergeLookupResult1);
			MergeLookupResult mergeLookupResult2 = new MergeLookupResult();
			mergeLookupResult2.setData(new HashMap<String, Object>() {{
				put("id2", 111);
				put("col1", "test");
			}});
			mergeLookupResult2.setDataExists(true);
			mergeLookupResult2.setTapTable(new TapTable("sub1"));
			MergeTableProperties mergeTableProperties3 = new MergeTableProperties();
			mergeTableProperties3.setMergeType(MergeTableProperties.MergeType.updateWrite);
			mergeTableProperties3.setJoinKeys(new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id2");
					put("target", "id2");
				}});
			}});
			mergeTableProperties3.setTargetPath("sub2");
			mergeLookupResult2.setProperty(mergeTableProperties3);
			mergeLookupResults.add(mergeLookupResult2);
			MergeFilter mergeFilter = new MergeFilter(false);
			MongodbMergeOperate.recursiveMerge(
					mergeBundle1,
					mergeTableProperties1,
					mergeResults,
					mergeLookupResults,
					null,
					null,
					mergeFilter,
					1,
					null
			);
			assertEquals(3, mergeResults.size());
		}

		@Test
		@DisplayName("U1 parent flatten DELETE should $unset child document targetPath and keep update not delete")
		void testParentDeleteUnsetChildTargetPath() {
			Map<String, Object> before = new HashMap<>();
			before.put("id", 1);
			before.put("colB", "b1");
			MergeTableProperties parent = new MergeTableProperties();
			parent.setMergeType(MergeTableProperties.MergeType.updateWrite);
			parent.setJoinKeys(new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id");
					put("target", "id");
				}});
			}});
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.DELETE, before, null);
			List<MergeResult> mergeResults = new ArrayList<>();
			MergeLookupResult child = new MergeLookupResult();
			child.setDataExists(true);
			child.setData(new HashMap<String, Object>() {{
				put("idC", 11);
				put("name", "part");
			}});
			child.setTapTable(new TapTable("level3"));
			MergeTableProperties childProp = new MergeTableProperties();
			childProp.setMergeType(MergeTableProperties.MergeType.updateWrite);
			childProp.setTargetPath("productPart");
			childProp.setJoinKeys(new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "idC");
					put("target", "idC");
				}});
			}});
			child.setProperty(childProp);
			List<MergeLookupResult> lookups = new ArrayList<>();
			lookups.add(child);
			MongodbMergeOperate.recursiveMerge(
					mergeBundle,
					parent,
					mergeResults,
					lookups,
					null,
					null,
					new MergeFilter(false),
					1,
					null
			);
			assertFalse(mergeResults.isEmpty());
			boolean unsetChildPath = false;
			boolean setChildPath = false;
			boolean deletedMain = false;
			for (MergeResult result : mergeResults) {
				if (result == null || result.getOperation() == null) {
					continue;
				}
				assertEquals(MergeResult.Operation.UPDATE, result.getOperation());
				assertEquals(1, result.getFilter().get("id"));
				Document update = result.getUpdate();
				if (update != null && update.containsKey("$unset") && update.get("$unset", Document.class).containsKey("productPart")) {
					unsetChildPath = true;
				}
				if (update != null && update.containsKey("$set") && update.get("$set", Document.class).keySet().stream().anyMatch(k -> k.equals("productPart") || k.startsWith("productPart."))) {
					setChildPath = true;
				}
				if (result.getOperation() == MergeResult.Operation.DELETE) {
					deletedMain = true;
				}
			}
			assertTrue(unsetChildPath);
			assertFalse(setChildPath);
			assertFalse(deletedMain);
		}

		@Test
		@DisplayName("U2 parent DELETE must not rewrite existing child lookup data as INSERT/$set")
		void testParentDeleteMustNotInsertChildLookup() {
			testParentDeleteUnsetChildTargetPath();
		}

		@Test
		@DisplayName("U5 parent flatten DELETE then descendant $unset/$pull executed in order must clean productPart and attrList of main doc")
		void testParentDeleteCleanDescendantsWhenExecutedInOrder() {
			Map<String, Object> before = new HashMap<>();
			before.put("_id", 101);
			before.put("PROD_ID", 101);
			before.put("itemId", 1);
			before.put("attrCode", "IRON");
			before.put("partName", "part-101");
			MergeTableProperties parent = new MergeTableProperties();
			parent.setMergeType(MergeTableProperties.MergeType.updateWrite);
			parent.setTargetPath("");
			parent.setJoinKeys(joinKeys("itemId", "itemId"));
			MergeTableProperties docChild = new MergeTableProperties();
			docChild.setMergeType(MergeTableProperties.MergeType.updateWrite);
			docChild.setTargetPath("productPart");
			docChild.setJoinKeys(joinKeys("referenceCode", "attrCode"));
			MergeTableProperties arrChild = new MergeTableProperties();
			arrChild.setMergeType(MergeTableProperties.MergeType.updateIntoArray);
			arrChild.setTargetPath("attrList");
			arrChild.setIsArray(false);
			arrChild.setArrayKeys(new ArrayList<>(Collections.singletonList("attrId")));
			arrChild.setJoinKeys(joinKeys("referenceCode", "attrCode"));
			List<MergeLookupResult> lookups = new ArrayList<>();
			lookups.add(lookup(docChild, "level3_doc", new Document("_id", 1).append("attrId", 1).append("referenceCode", "IRON").append("attrName", "Iron")));
			lookups.add(lookup(arrChild, "level3_arr", new Document("_id", 11).append("attrId", 11).append("referenceCode", "IRON").append("v", "a")));
			lookups.add(lookup(arrChild, "level3_arr", new Document("_id", 12).append("attrId", 12).append("referenceCode", "IRON").append("v", "b")));
			List<MergeResult> mergeResults = new ArrayList<>();
			MongodbMergeOperate.recursiveMerge(
					new MergeBundle(MergeBundle.EventOperation.DELETE, before, null),
					parent,
					mergeResults,
					lookups,
					null,
					new HashSet<>(Collections.singletonList("itemId")),
					new MergeFilter(true),
					2,
					null
			);

			Document main = new Document("itemId", 1).append("name", "item-1")
					.append("PROD_ID", 101).append("attrCode", "IRON").append("partName", "part-101")
					.append("productPart", new Document("_id", 1).append("attrId", 1).append("referenceCode", "IRON").append("attrName", "Iron"))
					.append("attrList", new ArrayList<>(Arrays.asList(
							new Document("_id", 11).append("attrId", 11).append("referenceCode", "IRON").append("v", "a"),
							new Document("_id", 12).append("attrId", 12).append("referenceCode", "IRON").append("v", "b"))));
			Document other = new Document("itemId", 2).append("name", "item-2")
					.append("PROD_ID", 102).append("attrCode", "GOLD").append("partName", "part-102")
					.append("productPart", new Document("_id", 2).append("attrId", 2).append("referenceCode", "GOLD"))
					.append("attrList", new ArrayList<>(Collections.singletonList(new Document("_id", 21).append("attrId", 21).append("referenceCode", "GOLD"))));
			List<Document> collection = Arrays.asList(main, other);
			for (MergeResult result : mergeResults) {
				if (result == null || result.getOperation() == null) {
					continue;
				}
				assertEquals(MergeResult.Operation.UPDATE, result.getOperation());
				applyInOrder(collection, result.getFilter(), result.getUpdate());
			}

			assertEquals(1, main.get("itemId"));
			assertEquals("item-1", main.get("name"));
			assertFalse(main.containsKey("PROD_ID"));
			assertFalse(main.containsKey("attrCode"));
			assertFalse(main.containsKey("partName"));
			assertFalse(main.containsKey("productPart"));
			Object attrList = main.get("attrList");
			assertTrue(null == attrList || ((List<?>) attrList).isEmpty(), "attrList should be pulled, actual: " + attrList);
			assertEquals("GOLD", other.get("attrCode"));
			assertTrue(other.containsKey("productPart"));
			assertEquals(1, ((List<?>) other.get("attrList")).size());
		}

		@Test
		@DisplayName("U6 parent flatten DELETE with array child as first lookup and its updateJoinKey before missing must still $pull attrList")
		void testParentDeleteArrayChildFirstLookupWithoutUpdateJoinKeyBefore() {
			Map<String, MergeInfo.UpdateJoinKey> updateJoinKeys = new HashMap<>();
			updateJoinKeys.put("arr", new MergeInfo.UpdateJoinKey(null, null, null));
			assertParentDeleteCleansArrayChildFirst(updateJoinKeys);
		}

		@Test
		@DisplayName("U6 parent flatten DELETE with array child as first lookup and updateJoinKeys null must still $pull attrList without NPE")
		void testParentDeleteArrayChildFirstLookupWithNullUpdateJoinKeys() {
			assertParentDeleteCleansArrayChildFirst(null);
		}

		private void assertParentDeleteCleansArrayChildFirst(Map<String, MergeInfo.UpdateJoinKey> updateJoinKeys) {
			Map<String, Object> before = new HashMap<>();
			before.put("_id", 101);
			before.put("PROD_ID", 101);
			before.put("itemId", 1);
			before.put("attrCode", "IRON");
			MergeTableProperties parent = new MergeTableProperties();
			parent.setId("parent");
			parent.setMergeType(MergeTableProperties.MergeType.updateWrite);
			parent.setTargetPath("");
			parent.setJoinKeys(joinKeys("itemId", "itemId"));
			MergeTableProperties arrChild = new MergeTableProperties();
			arrChild.setId("arr");
			arrChild.setMergeType(MergeTableProperties.MergeType.updateIntoArray);
			arrChild.setTargetPath("attrList");
			arrChild.setIsArray(false);
			arrChild.setArrayKeys(new ArrayList<>(Collections.singletonList("attrId")));
			arrChild.setJoinKeys(joinKeys("referenceCode", "attrCode"));
			MergeTableProperties docChild = new MergeTableProperties();
			docChild.setId("doc");
			docChild.setMergeType(MergeTableProperties.MergeType.updateWrite);
			docChild.setTargetPath("productPart");
			docChild.setJoinKeys(joinKeys("referenceCode", "attrCode"));
			List<MergeLookupResult> lookups = new ArrayList<>();
			lookups.add(lookup(arrChild, "level3_arr", new Document("_id", 11).append("attrId", 11).append("referenceCode", "IRON")));
			lookups.add(lookup(docChild, "level3_doc", new Document("_id", 1).append("attrId", 1).append("referenceCode", "IRON")));
			List<MergeResult> mergeResults = new ArrayList<>();
			assertDoesNotThrow(() -> MongodbMergeOperate.recursiveMerge(
					new MergeBundle(MergeBundle.EventOperation.DELETE, before, null),
					parent,
					mergeResults,
					lookups,
					updateJoinKeys,
					new HashSet<>(Collections.singletonList("itemId")),
					new MergeFilter(true),
					2,
					null
			));

			Document main = new Document("itemId", 1).append("PROD_ID", 101).append("attrCode", "IRON")
					.append("productPart", new Document("_id", 1).append("attrId", 1).append("referenceCode", "IRON"))
					.append("attrList", new ArrayList<>(Collections.singletonList(new Document("_id", 11).append("attrId", 11).append("referenceCode", "IRON"))));
			Document other = new Document("itemId", 2).append("attrCode", "GOLD")
					.append("attrList", new ArrayList<>(Collections.singletonList(new Document("_id", 21).append("attrId", 21).append("referenceCode", "GOLD"))));
			boolean pulled = false;
			for (MergeResult result : mergeResults) {
				if (result == null || result.getOperation() == null) {
					continue;
				}
				assertEquals(MergeResult.Operation.UPDATE, result.getOperation());
				Document update = result.getUpdate();
				assertFalse(update.containsKey("$set"), "parent DELETE must not $set child data: " + update.toJson());
				if (update.containsKey("$pull") && update.get("$pull", Document.class).containsKey("attrList")) {
					pulled = true;
				}
				applyInOrder(Arrays.asList(main, other), result.getFilter(), update);
			}
			assertTrue(pulled, "expected $pull attrList, actual writes: " + mergeResults);
			assertTrue(((List<?>) main.get("attrList")).isEmpty(), "attrList should be pulled, actual: " + main.get("attrList"));
			assertFalse(main.containsKey("productPart"));
			assertEquals(1, ((List<?>) other.get("attrList")).size());
		}

		@Test
		@DisplayName("U7 parent array DELETE with array child (both updateIntoArray) must still produce cleanup write for child, never INSERT/$set")
		void testParentArrayDeleteWithArrayChildStillCleans() {
			Map<String, Object> before = new HashMap<>();
			before.put("partId", 101);
			before.put("itemId", 1);
			before.put("attrCode", "IRON");
			MergeTableProperties parent = new MergeTableProperties();
			parent.setId("parentArr");
			parent.setMergeType(MergeTableProperties.MergeType.updateIntoArray);
			parent.setTargetPath("parts");
			parent.setIsArray(false);
			parent.setArrayKeys(new ArrayList<>(Collections.singletonList("partId")));
			parent.setJoinKeys(joinKeys("itemId", "itemId"));
			MergeTableProperties arrChild = new MergeTableProperties();
			arrChild.setId("arr");
			arrChild.setMergeType(MergeTableProperties.MergeType.updateIntoArray);
			arrChild.setTargetPath("attrList");
			arrChild.setIsArray(false);
			arrChild.setArrayKeys(new ArrayList<>(Collections.singletonList("attrId")));
			arrChild.setJoinKeys(joinKeys("referenceCode", "attrCode"));
			Map<String, MergeInfo.UpdateJoinKey> updateJoinKeys = new HashMap<>();
			updateJoinKeys.put("arr", new MergeInfo.UpdateJoinKey(null, null, null));
			List<MergeLookupResult> lookups = new ArrayList<>();
			lookups.add(lookup(arrChild, "level3_arr", new Document("_id", 11).append("attrId", 11).append("referenceCode", "IRON")));
			List<MergeResult> mergeResults = new ArrayList<>();
			assertDoesNotThrow(() -> MongodbMergeOperate.recursiveMerge(
					new MergeBundle(MergeBundle.EventOperation.DELETE, before, null),
					parent,
					mergeResults,
					lookups,
					updateJoinKeys,
					null,
					new MergeFilter(true),
					2,
					null
			));
			boolean parentPulled = false;
			boolean childCleaned = false;
			for (MergeResult result : mergeResults) {
				if (result == null || result.getOperation() == null) {
					continue;
				}
				assertEquals(MergeResult.Operation.UPDATE, result.getOperation());
				Document update = result.getUpdate();
				assertFalse(update.containsKey("$set"), "parent DELETE must not $set child data: " + update.toJson());
				Document pull = update.get("$pull", Document.class);
				Document unset = update.get("$unset", Document.class);
				if (pull != null && pull.containsKey("parts")) {
					parentPulled = true;
				}
				if ((pull != null && pull.containsKey("attrList")) || (unset != null && unset.containsKey("attrList"))) {
					childCleaned = true;
				}
			}
			assertTrue(parentPulled, "expected $pull parts, actual writes: " + mergeResults);
			assertTrue(childCleaned, "expected cleanup write for attrList, actual writes: " + mergeResults);
		}

		private List<Map<String, String>> joinKeys(String source, String target) {
			List<Map<String, String>> keys = new ArrayList<>();
			Map<String, String> key = new HashMap<>();
			key.put("source", source);
			key.put("target", target);
			keys.add(key);
			return keys;
		}

		private MergeLookupResult lookup(MergeTableProperties property, String table, Map<String, Object> data) {
			MergeLookupResult lookupResult = new MergeLookupResult();
			lookupResult.setDataExists(true);
			lookupResult.setData(data);
			lookupResult.setTapTable(new TapTable(table));
			lookupResult.setProperty(property);
			return lookupResult;
		}

		/**
		 * 按 Mongo 语义顺序执行合并写出的 UpdateOne（仅支持本场景用到的等值过滤、$unset、$pull），
		 * 用于发现「前序 $unset 掉的字段被后续写入当过滤条件」这类只有顺序执行才暴露的问题。
		 */
		private void applyInOrder(List<Document> collection, Document filter, Document update) {
			for (Document doc : collection) {
				boolean matched = true;
				for (Map.Entry<String, Object> condition : filter.entrySet()) {
					assertFalse(condition.getValue() instanceof Document, "unsupported filter operator: " + condition);
					if (!doc.containsKey(condition.getKey()) || !Objects.equals(doc.get(condition.getKey()), condition.getValue())) {
						matched = false;
						break;
					}
				}
				if (!matched) {
					continue;
				}
				for (Map.Entry<String, Object> op : update.entrySet()) {
					Document body = (Document) op.getValue();
					if ("$unset".equals(op.getKey())) {
						body.keySet().forEach(doc::remove);
					} else if ("$pull".equals(op.getKey())) {
						for (Map.Entry<String, Object> pull : body.entrySet()) {
							Object array = doc.get(pull.getKey());
							if (!(array instanceof List)) {
								continue;
							}
							Map<?, ?> condition = (Map<?, ?>) pull.getValue();
							((List<?>) array).removeIf(element -> element instanceof Map && condition.entrySet().stream()
									.allMatch(c -> Objects.equals(((Map<?, ?>) element).get(c.getKey()), c.getValue())));
						}
					} else {
						fail("unsupported update operator in parent DELETE cleanup: " + op.getKey());
					}
				}
			}
		}
	}

	@Nested
	@DisplayName("Method filterSetDocByUnsetDoc test")
	class filterSetDocByUnsetDocTest {
		@Test
		@DisplayName("main process test")
		void test1() {
			Document setDoc = new Document()
					.append("id", 1)
					.append("name", "test")
					.append("desc.city", "shanghai")
					.append("sub1", new Document())
					.append("sub2", new Document("f1", "test"));
			Document unsetDoc = new Document()
					.append("desc.city", true)
					.append("sub1.f1", true)
					.append("sub2.f1", true);
			Document actual = MongodbMergeOperate.filterSetDocByUnsetDoc(setDoc, unsetDoc);
			assertSame(actual, setDoc);
			assertEquals(3, actual.size());
			assertTrue(actual.containsKey("id"));
			assertTrue(actual.containsKey("name"));
			assertTrue(actual.containsKey("sub2"));
		}

		@Test
		@DisplayName("input set doc is null")
		void test2() {
			Document actual = MongodbMergeOperate.filterSetDocByUnsetDoc(null, new Document());
			assertNull(actual);
		}

		@Test
		@DisplayName("input unset doc is null")
		void test3() {
			Document setDoc = new Document()
					.append("id", 1)
					.append("name", "test");
			Document actual = MongodbMergeOperate.filterSetDocByUnsetDoc(setDoc, null);
			assertEquals(setDoc, actual);
		}
	}

	@Nested
	@DisplayName("Method updateIntoArrayMerge test")
	class updateIntoArrayMergeTest {
		@Test
		@DisplayName("test update into array, op: u, target path: 'arr', isArray: false, joinKey: {source: 'id', target: 'id'}, arrayKeys: ['id', 'index']")
		void test1() {
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.UPDATE, new HashMap<String, Object>() {{
				put("ID", 1);
				put("INDEX", 1);
				put("NAME", "test");
			}}, new HashMap<String, Object>() {{
				put("id", 2);
				put("index", 22);
				put("name", "test");
			}});
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateIntoArray);
			mergeTableProperties.setJoinKeys(new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id");
					put("target", "id");
				}});
			}});
			mergeTableProperties.setTargetPath("arr");
			mergeTableProperties.setArrayKeys(new ArrayList<String>() {{
				add("id");
				add("index");
			}});
			mergeTableProperties.setIsArray(false);
			MergeResult mergeResult = new MergeResult();
			MergeFilter mergeFilter = new MergeFilter(true);

			MongodbMergeOperate.updateIntoArrayMerge(mergeBundle, mergeTableProperties, mergeResult, mergeFilter);

			Document filter = mergeResult.getFilter();
			assertNotNull(filter);
			assertTrue(filter.containsKey("id"));
			assertEquals(2, filter.getInteger("id"));
			Document update = mergeResult.getUpdate();
			assertNotNull(update);
			assertTrue(update.containsKey("$set"));
			Document setDoc = update.get("$set", Document.class);
			assertNotNull(setDoc);
			assertEquals(3, setDoc.size());
			assertEquals(2, setDoc.getInteger("arr.$[element1].id"));
			assertEquals(22, setDoc.getInteger("arr.$[element1].index"));
			assertEquals("test", setDoc.getString("arr.$[element1].name"));
			UpdateOptions updateOptions = mergeResult.getUpdateOptions();
			assertNotNull(updateOptions);
			List<? extends Bson> arrayFilters = updateOptions.getArrayFilters();
			assertNotNull(arrayFilters);
			assertEquals(1, arrayFilters.size());
			Bson arrayFilter = arrayFilters.get(0);
			assertNotNull(arrayFilter);
			assertEquals(2, arrayFilter.toBsonDocument().getInt32("element1.id").getValue());
			assertEquals(22, arrayFilter.toBsonDocument().getInt32("element1.index").getValue());
		}
	}

	@Nested
	@DisplayName("Method updateMerge test")
	class updateMergeTest {
		@Test
		@DisplayName("test update merge, set: {\"id\": 1, \"model\": {}}, remove fields: {\"model\": 1}, set and unset both exists")
		void test1() {
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.UPDATE,
					new Document("id", 1),
					new Document("id", 1).append("model", new Document()));
			mergeBundle.setRemovefields(new Document("model", 1));
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			Map<String, String> joinKey = new HashMap<>();
			joinKey.put("source", "id");
			joinKey.put("target", "id");
			List<Map<String, String>> joinKeys = new ArrayList<>();
			joinKeys.add(joinKey);
			mergeTableProperties.setJoinKeys(joinKeys);
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			mergeTableProperties.setTargetPath("subMap");
			MergeResult mergeResult = new MergeResult();
			MongodbMergeOperate.updateMerge(mergeBundle, mergeTableProperties, mergeResult, new HashSet<>(), new MergeFilter(true));

			Document update = mergeResult.getUpdate();
			assertEquals("{\"$unset\": {\"subMap.model\": 1}, \"$set\": {\"subMap.id\": 1}}", update.toJson());
		}

		@Test
		@DisplayName("on the basis of test1, in the update in the mergeResult passed in, add $set")
		void test2() {
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.UPDATE,
					new Document("id", 1),
					new Document("id", 1).append("model", new Document()));
			mergeBundle.setRemovefields(new Document("model", 1));
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			Map<String, String> joinKey = new HashMap<>();
			joinKey.put("source", "id");
			joinKey.put("target", "id");
			List<Map<String, String>> joinKeys = new ArrayList<>();
			joinKeys.add(joinKey);
			mergeTableProperties.setJoinKeys(joinKeys);
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			mergeTableProperties.setTargetPath("subMap");
			MergeResult mergeResult = new MergeResult();
			mergeResult.setUpdate(new Document("$set", new Document("td", 1)));
			MongodbMergeOperate.updateMerge(mergeBundle, mergeTableProperties, mergeResult, new HashSet<>(), new MergeFilter(true));

			Document update = mergeResult.getUpdate();
			assertEquals("{\"$set\": {\"td\": 1, \"subMap.id\": 1}, \"$unset\": {\"subMap.model\": 1}}", update.toJson());
		}
	}

	@Nested
	@DisplayName("Method mergeBundle test")
	class mergeBundleTest {
		@Test
		@DisplayName("test insert event")
		void test1() {
			Document after = new Document("id", 1).append("title", "xxxxxxxx").append("lastModDate", Instant.now());
			List<String> removeFields = new ArrayList<>();
			removeFields.add("test");
			TapInsertRecordEvent insertRecordEvent = TapInsertRecordEvent.create().init().after(after).removedFields(removeFields);
			MergeBundle mergeBundle = MongodbMergeOperate.mergeBundle(insertRecordEvent);
			assertNotNull(mergeBundle);
			assertEquals(MergeBundle.EventOperation.INSERT, mergeBundle.getOperation());
			assertEquals(after, mergeBundle.getAfter());
			assertEquals(new HashMap<String, Object>() {
				{
					put("test", true);
				}
			}, mergeBundle.getRemovefields());
		}

		@Test
		@DisplayName("test update event")
		void test2() {
			Document before = new Document("id", 1).append("title", "xxxxxxxx").append("lastModDate", Instant.now()).append("test", 1);
			Document after = new Document("id", 1).append("title", "yyyyyyyy").append("lastModDate", Instant.now());
			List<String> removeFields = new ArrayList<>();
			removeFields.add("test");
			TapUpdateRecordEvent updateRecordEvent = TapUpdateRecordEvent.create().init().before(before).after(after).removedFields(removeFields);
			MergeBundle mergeBundle = MongodbMergeOperate.mergeBundle(updateRecordEvent);
			assertNotNull(mergeBundle);
			assertEquals(MergeBundle.EventOperation.UPDATE, mergeBundle.getOperation());
			assertEquals(before, mergeBundle.getBefore());
			assertEquals(after, mergeBundle.getAfter());
			assertEquals(new HashMap<String, Object>() {
				{
					put("test", true);
				}
			}, mergeBundle.getRemovefields());
		}

		@Test
		@DisplayName("test delete event")
		void test3() {
			Document before = new Document("id", 1).append("title", "xxxxxxxx").append("lastModDate", Instant.now());
			TapDeleteRecordEvent deleteRecordEvent = TapDeleteRecordEvent.create().init().before(before);
			MergeBundle mergeBundle = MongodbMergeOperate.mergeBundle(deleteRecordEvent);
			assertNotNull(mergeBundle);
			assertEquals(MergeBundle.EventOperation.DELETE, mergeBundle.getOperation());
			assertEquals(before, mergeBundle.getBefore());
		}
	}

	@Nested
	@DisplayName("Method buildUnsetDocument test")
	class buildUnsetDocumentTest {
		@Test
		@DisplayName("test not array, have target path")
		void test1() {
			Document data = new Document("f1", true)
					.append("subDoc", true)
					.append("subDoc.f1", true)
					.append("f2", true);
			Set<String> shareJoinKey = new HashSet<>();
			shareJoinKey.add("target.f2");
			Document unsetDoc = MongodbMergeOperate.buildUnsetDocument(shareJoinKey, data, "target", false, false);
			assertNotNull(unsetDoc);
			Document expect = new Document("target.f1", true)
					.append("target.subDoc", true);
			assertEquals(expect, unsetDoc);
		}

		@Test
		@DisplayName("test not array, not have target path")
		void test2() {
			Document data = new Document("f1", true)
					.append("subDoc", true)
					.append("subDoc.f1", true)
					.append("f2", true);
			Set<String> shareJoinKey = new HashSet<>();
			shareJoinKey.add("f2");
			Document unsetDoc = MongodbMergeOperate.buildUnsetDocument(shareJoinKey, data, "", false, false);
			assertNotNull(unsetDoc);
			Document expect = new Document("f1", true)
					.append("subDoc", true);
			assertEquals(expect, unsetDoc);
		}

		@Test
		@DisplayName("test array, have target path")
		void test3() {
			Document data = new Document("f1", true)
					.append("subArray", true)
					.append("subArray.f1", true)
					.append("f2", true);
			Set<String> shareJoinKey = new HashSet<>();
			shareJoinKey.add("target.f2");
			Document unsetDoc = MongodbMergeOperate.buildUnsetDocument(shareJoinKey, data, "target", true, true);
			assertNotNull(unsetDoc);
			Document expect = new Document("target.$[element1].f1", true)
					.append("target.$[element1].subArray", true);
			assertEquals(expect, unsetDoc);
		}

		@Test
		@DisplayName("test array, not have target path")
		void test4() {
			Document data = new Document("f1", true)
					.append("subArray", true)
					.append("subArray.f1", true)
					.append("f2", true);
			Set<String> shareJoinKey = new HashSet<>();
			shareJoinKey.add("target.f2");
			Document unsetDoc = MongodbMergeOperate.buildUnsetDocument(shareJoinKey, data, "", true, true);
			assertTrue(unsetDoc.isEmpty());
		}

		@Test
		@DisplayName("test array, firstMergeResult is false")
		void test5() {
			Document data = new Document("f1", true)
					.append("subArray", true)
					.append("subArray.f1", true)
					.append("f2", true);
			Set<String> shareJoinKey = new HashSet<>();
			shareJoinKey.add("target.f2");
			Document unsetDoc = MongodbMergeOperate.buildUnsetDocument(shareJoinKey, data, "target", true, false);
			assertTrue(unsetDoc.isEmpty());
		}
	}

	@Nested
	@DisplayName("Method unsetFilter test")
	class unsetFilterTest {

		private Document before;
		private Document after;
		private List<Map<String, String>> joinKeys;

		@BeforeEach
		void setUp() {
			before = new Document("id1", 1)
					.append("type1", "xxx")
					.append("f1", "zzzz")
					.append("f2", 625.85);
			after = new Document("id1", 2)
					.append("type1", "zzz")
					.append("f1", "yyyy")
					.append("f2", 123.45);
			joinKeys = new ArrayList<>();
			joinKeys.add(new HashMap<String, String>() {{
				put("source", "id");
				put("target", "id1");
			}});
			joinKeys.add(new HashMap<String, String>() {{
				put("source", "type");
				put("target", "type1");
			}});
		}

		@Test
		@DisplayName("test main process")
		void test1() {
			Document filter = MongodbMergeOperate.unsetFilter(before, after, joinKeys, 1);
			assertEquals(new Document("id1", 2).append("type1", "zzz"), filter);
		}
	}

	@Nested
	@DisplayName("Method updateIntoArrayUnsetMerge test")
	class updateIntoArrayUnsetMergeTest {
		@Test
		@DisplayName("U6 updateJoinKeys null with existing mergeResult under updateWrite parent must not NPE")
		void testNullUpdateJoinKeysNoNpe() {
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.DELETE, new HashMap<String, Object>() {{
				put("attrId", 11);
			}}, null);
			MergeTableProperties currentProperty = new MergeTableProperties();
			currentProperty.setId("arr");
			currentProperty.setTargetPath("attrList");
			currentProperty.setMergeType(MergeTableProperties.MergeType.updateIntoArray);
			MergeTableProperties parentProperties = new MergeTableProperties();
			parentProperties.setId("parent");
			parentProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			MergeResult mergeResult = new MergeResult();
			assertDoesNotThrow(() -> MongodbMergeOperate.updateIntoArrayUnsetMerge(mergeBundle, currentProperty, null, mergeResult, parentProperties, new MergeFilter(true), 1));
		}

		@Test
		@DisplayName("test data exists")
		void test1() {
			Map<String, Object> before = new HashMap<>();
			before.put("id", 1);
			before.put("src", "x");
			before.put("seq", 1);
			before.put("name", "test");
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("src", "y");
			after.put("seq", 1);
			after.put("name", "test1");
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.UPDATE, before, after);
			mergeBundle.setDataExists(true);
			MergeTableProperties currentProperty = new MergeTableProperties();
			currentProperty.setId("1");
			currentProperty.setTargetPath("array");
			currentProperty.setJoinKeys(new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id");
					put("target", "id");
				}});
				add(new HashMap<String, String>() {{
					put("source", "src");
					put("target", "src");
				}});
			}});
			currentProperty.setArrayKeys(new ArrayList<String>() {{
				add("id");
				add("src");
				add("seq");
			}});
			currentProperty.setMergeType(MergeTableProperties.MergeType.updateIntoArray);
			Map<String, MergeInfo.UpdateJoinKey> updateJoinKeys = new HashMap<>();
			MergeInfo.UpdateJoinKey updateJoinKey = new MergeInfo.UpdateJoinKey(new Document("id", 1).append("src", "x"), new Document("id", 1).append("src", "y"), new Document("id", 1).append("src", "x"));
			updateJoinKeys.put("1", updateJoinKey);
			MergeResult mergeResult = new MergeResult();
			MergeTableProperties parentProperties = new MergeTableProperties();
			parentProperties.setId("2");
			parentProperties.setMergeType(MergeTableProperties.MergeType.updateOrInsert);
			MergeFilter mergeFilter = new MergeFilter(true);
			MergeResult result = MongodbMergeOperate.updateIntoArrayUnsetMerge(mergeBundle, currentProperty, updateJoinKeys, mergeResult, parentProperties, mergeFilter, 1);
			assertEquals("{\"id\": 1, \"src\": \"x\"}", result.getFilter().toJson());
			assertEquals("{\"$pull\": {\"array\": {\"id\": 1, \"src\": \"x\", \"seq\": 1}}}", result.getUpdate().toJson());
		}

		@Test
		@DisplayName("test data not exists")
		void test2() {
			Map<String, Object> before = new HashMap<>();
			before.put("id", 1);
			before.put("src", "x");
			before.put("seq", 1);
			before.put("name", "test");
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("src", "y");
			after.put("seq", 1);
			after.put("name", "test1");
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.UPDATE, before, after);
			mergeBundle.setDataExists(false);
			MergeTableProperties currentProperty = new MergeTableProperties();
			currentProperty.setId("1");
			currentProperty.setTargetPath("array");
			currentProperty.setJoinKeys(new ArrayList<Map<String, String>>() {{
				add(new HashMap<String, String>() {{
					put("source", "id");
					put("target", "id");
				}});
				add(new HashMap<String, String>() {{
					put("source", "src");
					put("target", "src");
				}});
			}});
			currentProperty.setArrayKeys(new ArrayList<String>() {{
				add("id");
				add("src");
				add("seq");
			}});
			currentProperty.setMergeType(MergeTableProperties.MergeType.updateIntoArray);
			Map<String, MergeInfo.UpdateJoinKey> updateJoinKeys = new HashMap<>();
			MergeInfo.UpdateJoinKey updateJoinKey = new MergeInfo.UpdateJoinKey(new Document("id", 1).append("src", "x"), new Document("id", 1).append("src", "y"), new Document("id", 1).append("src", "x"));
			updateJoinKeys.put("1", updateJoinKey);
			MergeResult mergeResult = new MergeResult();
			MergeTableProperties parentProperties = new MergeTableProperties();
			parentProperties.setId("2");
			parentProperties.setMergeType(MergeTableProperties.MergeType.updateOrInsert);
			MergeFilter mergeFilter = new MergeFilter(true);
			MergeResult result = MongodbMergeOperate.updateIntoArrayUnsetMerge(mergeBundle, currentProperty, updateJoinKeys, mergeResult, parentProperties, mergeFilter, 1);
			assertEquals("{\"id\": 1, \"src\": \"x\"}", result.getFilter().toJson());
			assertEquals("{\"$pull\": {\"array\": {\"id\": 1, \"src\": \"x\", \"seq\": 1}}}", result.getUpdate().toJson());
		}
	}

	@Nested
	@DisplayName("Method removeIdIfNeed test")
	class removeIdIfNeedTest {
		@Test
		@DisplayName("test main process")
		void test1() {
			Map<String, Object> data = new HashMap<>();
			data.put("_id", new ObjectId());
			data.put("f1", 1);
			data.put("f2", 1);
			data.put("f3", 1);
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			List<Map<String, String>> joinKeys = new ArrayList<>();
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "f1");
				put("target", "f1");
			}});
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "f2");
				put("target", "f2");
			}});
			mergeTableProperties.setJoinKeys(joinKeys);
			MongodbMergeOperate.removeIdIfNeed(data, mergeTableProperties);
			assertFalse(data.containsKey("_id"));
		}

		@Test
		@DisplayName("test merge type is updateIntoArray")
		void test2() {
			Map<String, Object> data = new HashMap<>();
			data.put("_id", new ObjectId());
			data.put("f1", 1);
			data.put("f2", 1);
			data.put("f3", 1);
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateIntoArray);
			List<Map<String, String>> joinKeys = new ArrayList<>();
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "f1");
				put("target", "f1");
			}});
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "f2");
				put("target", "f2");
			}});
			mergeTableProperties.setJoinKeys(joinKeys);
			MongodbMergeOperate.removeIdIfNeed(data, mergeTableProperties);
			assertTrue(data.containsKey("_id"));
		}

		@Test
		@DisplayName("test target path is not empty")
		void test3() {
			Map<String, Object> data = new HashMap<>();
			data.put("_id", new ObjectId());
			data.put("f1", 1);
			data.put("f2", 1);
			data.put("f3", 1);
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			List<Map<String, String>> joinKeys = new ArrayList<>();
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "f1");
				put("target", "f1");
			}});
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "f2");
				put("target", "f2");
			}});
			mergeTableProperties.setJoinKeys(joinKeys);
			mergeTableProperties.setTargetPath("xxx");
			MongodbMergeOperate.removeIdIfNeed(data, mergeTableProperties);
			assertTrue(data.containsKey("_id"));
		}

		@Test
		@DisplayName("test data is empty")
		void test4() {
			Map<String, Object> data = new HashMap<>();
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			List<Map<String, String>> joinKeys = new ArrayList<>();
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "f1");
				put("target", "f1");
			}});
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "f2");
				put("target", "f2");
			}});
			mergeTableProperties.setJoinKeys(joinKeys);
			mergeTableProperties.setTargetPath("xxx");
			assertDoesNotThrow(() -> MongodbMergeOperate.removeIdIfNeed(data, mergeTableProperties));
		}

		@Test
		@DisplayName("test merge table properties is null")
		void test5() {
			Map<String, Object> data = new HashMap<>();
			data.put("_id", new ObjectId());
			data.put("f1", 1);
			data.put("f2", 1);
			data.put("f3", 1);
			MongodbMergeOperate.removeIdIfNeed(data, null);
			assertTrue(data.containsKey("_id"));
		}

		@Test
		@DisplayName("test join keys contains value _id")
		void test6() {
			Map<String, Object> data = new HashMap<>();
			data.put("_id", new ObjectId());
			data.put("f1", 1);
			data.put("f2", 1);
			data.put("f3", 1);
			MergeTableProperties mergeTableProperties = new MergeTableProperties();
			mergeTableProperties.setMergeType(MergeTableProperties.MergeType.updateWrite);
			List<Map<String, String>> joinKeys = new ArrayList<>();
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "f1");
				put("target", "f1");
			}});
			joinKeys.add(new HashMap<String, String>(){{
				put("source", "_id");
				put("target", "f2");
			}});
			mergeTableProperties.setJoinKeys(joinKeys);
			MongodbMergeOperate.removeIdIfNeed(data, mergeTableProperties);
			assertTrue(data.containsKey("_id"));
		}
	}

	@Nested
	@DisplayName("Method targetPath test")
	class targetPathTest {
		@Test
		@DisplayName("test simple variable substitution")
		void testSimpleVariableSubstitution() {
			Map<String, Object> data = new HashMap<>();
			data.put("txn_id", 1);
			data.put("item_id", 1);

			String targetPath = "txn_array_${txn_id}_${item_id}";
			String result = dynamicKey(targetPath, data);

			assertEquals("txn_array_1_1", result);
		}

		@Test
		@DisplayName("test nested variable substitution")
		void testNestedVariableSubstitution() {
			Map<String, Object> data = new HashMap<>();
			Map<String, Object> nested = new HashMap<>();
			nested.put("id", 123);
			data.put("transaction", nested);
			data.put("user_id", "user456");

			String targetPath = "user_${user_id}_txn_${transaction.id}";
			String result = dynamicKey(targetPath, data);

			assertEquals("user_user456_txn_123", result);
		}

		@Test
		@DisplayName("test no variables in targetPath")
		void testNoVariables() {
			Map<String, Object> data = new HashMap<>();
			data.put("txn_id", 1);

			String targetPath = "simple_path";
			String result = dynamicKey(targetPath, data);

			assertEquals("simple_path", result);
		}

		@Test
		@DisplayName("test variable not found in data")
		void testVariableNotFound() {
			Map<String, Object> data = new HashMap<>();
			data.put("txn_id", 1);

			String targetPath = "txn_array_${txn_id}_${missing_id}";
			String result = dynamicKey(targetPath, data);

			assertEquals("txn_array_1_${missing_id}", result);
		}

		@Test
		@DisplayName("test empty targetPath")
		void testEmptyTargetPath() {
			Map<String, Object> data = new HashMap<>();
			data.put("txn_id", 1);

			String result = dynamicKey("", data);

			assertEquals("", result);
		}

		@Test
		@DisplayName("test null targetPath")
		void testNullTargetPath() {
			Map<String, Object> data = new HashMap<>();
			data.put("txn_id", 1);

			String result = dynamicKey(null, data);

			assertNull(result);
		}

		@Test
		@DisplayName("test empty data")
		void testEmptyData() {
			Map<String, Object> data = new HashMap<>();

			String targetPath = "txn_array_${txn_id}_${item_id}";
			String result = dynamicKey(targetPath, data);

			assertEquals("txn_array_${txn_id}_${item_id}", result);
		}

		@Test
		@DisplayName("test null data")
		void testNullData() {
			String targetPath = "txn_array_${txn_id}_${item_id}";
			String result = dynamicKey(targetPath, null);

			assertEquals("txn_array_${txn_id}_${item_id}", result);
		}

		@Test
		@DisplayName("test multiple same variables")
		void testMultipleSameVariables() {
			Map<String, Object> data = new HashMap<>();
			data.put("id", 42);

			String targetPath = "prefix_${id}_middle_${id}_suffix";
			String result = dynamicKey(targetPath, data);

			assertEquals("prefix_42_middle_42_suffix", result);
		}

		@Test
		@DisplayName("test variable with null value")
		void testVariableWithNullValue() {
			Map<String, Object> data = new HashMap<>();
			data.put("txn_id", null);
			data.put("item_id", 1);

			String targetPath = "txn_array_${txn_id}_${item_id}";
			String result = dynamicKey(targetPath, data);

			assertEquals("txn_array_${txn_id}_1", result);
		}
	}
}
