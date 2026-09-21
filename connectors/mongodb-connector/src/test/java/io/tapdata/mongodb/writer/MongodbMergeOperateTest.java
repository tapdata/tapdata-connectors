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
		@DisplayName("TAP-12967 case 9a: an empty child filter can be supplemented by the parent conditions")
		void testEmptyFilterSupplementedFromParent() {
			MergeResult mergeResult = new MergeResult();
			MergeFilter mergeFilter = new MergeFilter(true);
			mergeFilter.addFilter(new Document("id1", 2));
			MongodbMergeOperate.appendAllParentMergeFilters(mergeResult, mergeFilter);

			assertEquals(1, mergeResult.getFilter().size());
			assertEquals(2, mergeResult.getFilter().getInteger("id1"));
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

		@Test
		@DisplayName("TAP-12967 case 1: insert into a nested array, isArray: true, arrayPath: 'ENROLLMENT', expect a non empty document level filter and unchanged arrayFilters")
		void testNestedArrayInsert() {
			Map<String, Object> after = new Document("enroll_id", 1001)
					.append("course_id", "C1")
					.append("name", "math");
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.INSERT, null, after);
			MergeTableProperties mergeTableProperties = arrayNodeProperties("ENROLLMENT",
					Arrays.asList(joinKey("enroll_id", "ENROLLMENT.enroll_id")));
			mergeTableProperties.setTargetPath("ENROLLMENT.COURSE");
			mergeTableProperties.setArrayKeys(new ArrayList<>(Collections.singletonList("enroll_id")));
			MergeResult mergeResult = new MergeResult();

			MongodbMergeOperate.updateIntoArrayMerge(mergeBundle, mergeTableProperties, mergeResult, new MergeFilter(true));

			Document filter = mergeResult.getFilter();
			assertFalse(filter.isEmpty());
			// TAP-12967: the document level condition is always arrayPath + "." + <element key>, never an $or branch
			assertEquals(new Document("ENROLLMENT.enroll_id", 1001), filter);
			assertFalse(filter.containsKey("$or"));
			assertFalse(filter.containsKey("$and"));

			// the arrayFilters are byte-for-byte the same as before the fix
			assertEquals("{\"element1.enroll_id\": 1001}", firstArrayFilterJson(mergeResult));

			Document addToSet = mergeResult.getUpdate().get("$addToSet", Document.class);
			assertNotNull(addToSet);
			assertEquals(after, addToSet.get("ENROLLMENT.$[element1].COURSE"));
		}

		@Test
		@DisplayName("TAP-12967 case 2: update in a nested array, isArray: true, expect a non empty document level filter and the $[element1].$[element2] path")
		void testNestedArrayUpdate() {
			Map<String, Object> before = new Document("enroll_id", 1001).append("course_id", "C1").append("name", "old");
			Map<String, Object> after = new Document("enroll_id", 1001).append("course_id", "C1").append("name", "new");
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.UPDATE, before, after);
			MergeTableProperties mergeTableProperties = arrayNodeProperties("ENROLLMENT",
					Arrays.asList(joinKey("enroll_id", "ENROLLMENT.enroll_id")));
			mergeTableProperties.setTargetPath("ENROLLMENT.COURSE");
			mergeTableProperties.setArrayKeys(new ArrayList<>(Collections.singletonList("course_id")));
			MergeResult mergeResult = new MergeResult();

			MongodbMergeOperate.updateIntoArrayMerge(mergeBundle, mergeTableProperties, mergeResult, new MergeFilter(true));

			assertEquals(1001, mergeResult.getFilter().getInteger("ENROLLMENT.enroll_id"));
			List<? extends Bson> arrayFilters = mergeResult.getUpdateOptions().getArrayFilters();
			assertEquals(2, arrayFilters.size());
			assertEquals(1001, arrayFilters.get(0).toBsonDocument().getInt32("element1.enroll_id").getValue());
			assertEquals("C1", arrayFilters.get(1).toBsonDocument().getString("element2.course_id").getValue());

			Document setDoc = mergeResult.getUpdate().get("$set", Document.class);
			assertNotNull(setDoc);
			assertEquals("new", setDoc.getString("ENROLLMENT.$[element1].COURSE.$[element2].name"));
		}

		@Test
		@DisplayName("TAP-12967 case 2b: joinKey.source overlaps arrayKeys and the value changed from before to after, the document level filter must use the before value, like the arrayFilters (review X2)")
		void testNestedArrayUpdateValueOverride() {
			Map<String, Object> before = new Document("enroll_id", 1001).append("course_id", "C1");
			Map<String, Object> after = new Document("enroll_id", 2002).append("course_id", "C1").append("name", "new");
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.UPDATE, before, after);
			MergeTableProperties mergeTableProperties = arrayNodeProperties("ENROLLMENT",
					Arrays.asList(joinKey("enroll_id", "ENROLLMENT.enroll_id")));
			mergeTableProperties.setTargetPath("ENROLLMENT.COURSE");
			mergeTableProperties.setArrayKeys(new ArrayList<>(Collections.singletonList("enroll_id")));
			MergeResult mergeResult = new MergeResult();

			MongodbMergeOperate.updateIntoArrayMerge(mergeBundle, mergeTableProperties, mergeResult, new MergeFilter(true));

			List<? extends Bson> arrayFilters = mergeResult.getUpdateOptions().getArrayFilters();
			assertEquals(2, arrayFilters.size());
			// the element level conditions use the before value, the document level one must do the same
			assertEquals(1001, arrayFilters.get(0).toBsonDocument().getInt32("element1.enroll_id").getValue());
			assertEquals(1001, arrayFilters.get(1).toBsonDocument().getInt32("element2.enroll_id").getValue());
			assertEquals(1001, mergeResult.getFilter().getInteger("ENROLLMENT.enroll_id"));
		}

		@Test
		@DisplayName("TAP-12967 case 3: delete from a nested array, isArray: true, the $pull must not run against an empty document level filter")
		void testNestedArrayDelete() {
			Map<String, Object> before = new Document("enroll_id", 1001).append("course_id", "C1");
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.DELETE, before, null);
			MergeTableProperties mergeTableProperties = arrayNodeProperties("ENROLLMENT",
					Arrays.asList(joinKey("enroll_id", "ENROLLMENT.enroll_id")));
			mergeTableProperties.setTargetPath("ENROLLMENT.COURSE");
			mergeTableProperties.setArrayKeys(new ArrayList<>(Collections.singletonList("enroll_id")));
			MergeResult mergeResult = new MergeResult();

			MongodbMergeOperate.updateIntoArrayMerge(mergeBundle, mergeTableProperties, mergeResult, new MergeFilter(true));

			assertFalse(mergeResult.getFilter().isEmpty());
			assertEquals(1001, mergeResult.getFilter().getInteger("ENROLLMENT.enroll_id"));
			Document pull = mergeResult.getUpdate().get("$pull", Document.class);
			assertNotNull(pull);
			assertEquals(new Document("enroll_id", 1001), pull.get("ENROLLMENT.$[element1].COURSE"));
		}

		@Test
		@DisplayName("TAP-12967 case 9b: append mode with parent filters, the parent conditions are merged into the array branch filter without overwriting it")
		void testNestedArrayInsertWithParentFilters() {
			Map<String, Object> after = new Document("enroll_id", 1001).append("course_id", "C1");
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.INSERT, null, after);
			MergeTableProperties mergeTableProperties = arrayNodeProperties("ENROLLMENT",
					Arrays.asList(joinKey("enroll_id", "ENROLLMENT.enroll_id")));
			mergeTableProperties.setTargetPath("ENROLLMENT.COURSE");
			mergeTableProperties.setArrayKeys(new ArrayList<>(Collections.singletonList("enroll_id")));
			MergeFilter mergeFilter = new MergeFilter(true);
			mergeFilter.addFilter(new Document("student_id", 9));
			mergeFilter.addFilter(new Document("ENROLLMENT.enroll_id", 999));
			MergeResult mergeResult = new MergeResult();

			MongodbMergeOperate.updateIntoArrayMerge(mergeBundle, mergeTableProperties, mergeResult, mergeFilter);

			assertEquals(2, mergeResult.getFilter().size());
			assertEquals(9, mergeResult.getFilter().getInteger("student_id"));
			// the document level condition derived from the row itself wins, the parent one is de-duplicated by key
			assertEquals(1001, mergeResult.getFilter().getInteger("ENROLLMENT.enroll_id"));
		}
	}

	@Nested
	@DisplayName("Method updateWriteUnsetMerge Test")
	class updateWriteUnsetMergeTest {

		@Test
		@DisplayName("TAP-12967 case 10: isArray: true and an empty unset filter, the filter is filled from the row itself and from the parent conditions, and the unset write model is emitted")
		void testEmptyFilterEarlyReturn() {
			Map<String, Object> after = new Document("enroll_id", 1001).append("course_id", "C1");
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.UPDATE, new Document("enroll_id", 1001), after);
			MergeTableProperties currentProperty = new MergeTableProperties();
			currentProperty.setId("1");
			currentProperty.setMergeType(MergeTableProperties.MergeType.updateWrite);
			currentProperty.setIsArray(true);
			currentProperty.setArrayPath("ENROLLMENT");
			currentProperty.setTargetPath("ENROLLMENT.COURSE");
			currentProperty.setJoinKeys(arrayNodeProperties("ENROLLMENT",
					Arrays.asList(joinKey("enroll_id", "ENROLLMENT.enroll_id"))).getJoinKeys());
			// the join key target is not a key of the before image, so unsetFilter produces an empty filter
			Map<String, MergeInfo.UpdateJoinKey> updateJoinKeys = new HashMap<>();
			updateJoinKeys.put("1", new MergeInfo.UpdateJoinKey(new Document("enroll_id", 1001),
					new Document("enroll_id", 1002), null));
			MergeFilter mergeFilter = new MergeFilter(true);
			mergeFilter.addFilter(new Document("student_id", 9));

			MergeResult result = MongodbMergeOperate.updateWriteUnsetMerge(mergeBundle, currentProperty, updateJoinKeys,
					null, new HashSet<>(), mergeFilter, 1, 1, new HashSet<>());

			assertNotNull(result);
			// the arrayFilters must never be written together with a completely empty document level filter
			assertFalse(result.getFilter().isEmpty());
			assertEquals(1001, result.getFilter().getInteger("ENROLLMENT.enroll_id"));
			assertEquals(9, result.getFilter().getInteger("student_id"));
			List<? extends Bson> arrayFilters = result.getUpdateOptions().getArrayFilters();
			assertEquals(1, arrayFilters.size());
			assertEquals(1001, arrayFilters.get(0).toBsonDocument().getInt32("element1.enroll_id").getValue());
			// TAP-12967: a result whose operation is left unset is discarded by the caller (addUnsetMerge
			// requires a non null operation), so the unset would silently be lost. It must be a real write.
			assertEquals(MergeResult.Operation.UPDATE, result.getOperation());
			assertTrue(result.getUpdate().containsKey(MongodbMergeOperate.UNSET_KEY));
			assertFalse(result.getUpdate().get(MongodbMergeOperate.UNSET_KEY, Document.class).isEmpty());
		}

		@Test
		@DisplayName("TAP-12967 case 10: isArray: true and an empty unset filter without any parent condition, the filter stays empty and the operation stays unset so that the caller discards the result")
		void testEmptyFilterWithoutParentFilters() {
			Map<String, Object> after = new Document("course_id", "C1");
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.UPDATE, new Document("course_id", "C1"), after);
			MergeTableProperties currentProperty = new MergeTableProperties();
			currentProperty.setId("1");
			currentProperty.setMergeType(MergeTableProperties.MergeType.updateWrite);
			currentProperty.setIsArray(true);
			currentProperty.setArrayPath("ENROLLMENT");
			currentProperty.setTargetPath("ENROLLMENT.COURSE");
			currentProperty.setJoinKeys(arrayNodeProperties("ENROLLMENT",
					Arrays.asList(joinKey("enroll_id", "ENROLLMENT.enroll_id"))).getJoinKeys());
			Map<String, MergeInfo.UpdateJoinKey> updateJoinKeys = new HashMap<>();
			updateJoinKeys.put("1", new MergeInfo.UpdateJoinKey(new Document("course_id", "C1"),
					new Document("course_id", "C2"), null));

			MergeResult result = MongodbMergeOperate.updateWriteUnsetMerge(mergeBundle, currentProperty, updateJoinKeys,
					null, new HashSet<>(), new MergeFilter(true), 1, 1, new HashSet<>());

			assertNotNull(result);
			assertTrue(result.getFilter().isEmpty());
			assertNotNull(result.getUpdateOptions().getArrayFilters());
			// TAP-12967: no condition can be derived from the row itself nor from the parents, so keep the
			// previous behaviour of the empty filter guard: the operation stays unset and the caller
			// (addUnsetMerge) discards the result, instead of emitting an unset matching the whole collection
			assertNull(result.getOperation());
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
		@DisplayName("TAP-12967: on an array node the derived document filter never overwrites a condition carried by the parent level")
		void testArrayNodeFilterDoesNotOverwriteParentCondition() {
			MergeTableProperties currentProperty = arrayNodeProperties("ENROLLMENT",
					Arrays.asList(joinKey("enroll_id", "ENROLLMENT.enroll_id")));
			currentProperty.setId("1");
			currentProperty.setTargetPath("ENROLLMENT.COURSE");
			Map<String, MergeInfo.UpdateJoinKey> updateJoinKeys = new HashMap<>();
			updateJoinKeys.put("1", new MergeInfo.UpdateJoinKey(new Document("enroll_id", 1001),
					new Document("enroll_id", 1002), new Document("student_id", 9)));
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.UPDATE,
					new Document("enroll_id", 1001), new Document("enroll_id", 1002));
			// the parent level has already identified the document to touch
			MergeResult mergeResult = new MergeResult();
			mergeResult.getFilter().put("ENROLLMENT.enroll_id", 777);

			MergeResult result = MongodbMergeOperate.updateIntoArrayUnsetMerge(mergeBundle, currentProperty,
					updateJoinKeys, mergeResult, null, new MergeFilter(true), 1);

			// the condition of the parent level wins, the derived one must not replace it
			assertEquals(777, result.getFilter().get("ENROLLMENT.enroll_id"));
			// the element level condition still carries the value of this row
			List<? extends Bson> arrayFilters = result.getUpdateOptions().getArrayFilters();
			assertEquals(1, arrayFilters.size());
			assertEquals(1001, arrayFilters.get(0).toBsonDocument().getInt32("element1.enroll_id").getValue());
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

		@Test
		@DisplayName("TAP-12967 case 4: isArray: true, the array branch must also produce a document level filter")
		void test3ArrayBranch() {
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
			currentProperty.setArrayPath("array");
			currentProperty.setIsArray(true);
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

			assertNotNull(result);
			assertFalse(result.getFilter().isEmpty());
			// data is the same map as the arrayFilters above, the document level conditions are flat
			assertEquals(new Document("array.id", 1).append("array.src", "x"), result.getFilter());
			assertFalse(result.getFilter().containsKey("$and"));
			assertFalse(result.getFilter().containsKey("$or"));
			List<? extends Bson> arrayFilters = result.getUpdateOptions().getArrayFilters();
			assertEquals(1, arrayFilters.size());
			assertEquals(1, arrayFilters.get(0).toBsonDocument().getInt32("element1.id").getValue());
			assertEquals("x", arrayFilters.get(0).toBsonDocument().getString("element1.src").getValue());
			// record the current behaviour of the array branch: the whole targetPath is set to an empty array
			Document setDoc = result.getUpdate().get("$set", Document.class);
			assertNotNull(setDoc);
			assertEquals(1, setDoc.size());
			assertEquals(new ArrayList<>(), setDoc.get("array"));
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
	@Nested
	@DisplayName("Method documentFilterForArrayNode Test")
	class documentFilterForArrayNodeTest {

		@Test
		@DisplayName("multiple join keys become an AND inside one flat Document, never an $and/$or")
		void testMultipleJoinKeysFlatAndStructure() {
			MergeTableProperties properties = arrayNodeProperties("ENROLLMENT",
					Arrays.asList(joinKey("a", "ENROLLMENT.a"), joinKey("b", "b")));
			Document filter = MongodbMergeOperate.documentFilterForArrayNode(new Document("a", 1).append("b", 2), properties);

			// one flat Document: every key is a condition, different keys are AND-ed implicitly, both target
			// conventions (with and without the arrayPath prefix) map to the very same document level path
			assertEquals(new Document("ENROLLMENT.a", 1).append("ENROLLMENT.b", 2), filter);
			assertFalse(filter.containsKey("$and"));
			assertFalse(filter.containsKey("$or"));
		}

		@Test
		@DisplayName("every join key keeps a single candidate, the result is a flat Document")
		void testFlatDocument() {
			MergeTableProperties properties = arrayNodeProperties("ENROLLMENT",
					Arrays.asList(joinKey("a", "ENROLLMENT.a"), joinKey("c", "ENROLLMENT.c")));
			Document filter = MongodbMergeOperate.documentFilterForArrayNode(new Document("a", 1).append("c", 3), properties);

			assertFalse(filter.containsKey("$and"));
			assertFalse(filter.containsKey("$or"));
			assertEquals(new Document("ENROLLMENT.a", 1).append("ENROLLMENT.c", 3), filter);
		}

		@Test
		@DisplayName("missing join key values are dropped, all values missing produces an empty filter")
		void testMissingValues() {
			MergeTableProperties properties = arrayNodeProperties("ENROLLMENT",
					Arrays.asList(joinKey("a", "ENROLLMENT.a"), joinKey("b", "ENROLLMENT.b"), joinKey("c", "ENROLLMENT.c")));
			Document filter = MongodbMergeOperate.documentFilterForArrayNode(new Document("a", 1).append("b", null), properties);

			assertEquals(new Document("ENROLLMENT.a", 1), filter);
			assertFalse(filter.containsKey("ENROLLMENT.b"));
			assertFalse(filter.containsKey("ENROLLMENT.c"));

			assertTrue(MongodbMergeOperate.documentFilterForArrayNode(new HashMap<>(), properties).isEmpty());
			assertTrue(MongodbMergeOperate.documentFilterForArrayNode(null, properties).isEmpty());
			assertTrue(MongodbMergeOperate.documentFilterForArrayNode(new Document("a", 1), null).isEmpty());
		}

		@Test
		@DisplayName("both target conventions are supported")
		void testTargetConventions() {
			Document filter = MongodbMergeOperate.documentFilterForArrayNode(new Document("enroll_id", 1001),
					arrayNodeProperties("ENROLLMENT", Arrays.asList(joinKey("enroll_id", "enroll_id"))));
			// target does not carry the arrayPath prefix: after stripping (getArrayMatchString) it is the very
			// same element key, so both conventions map to the same document level path
			assertEquals(new Document("ENROLLMENT.enroll_id", 1001), filter);
			assertFalse(filter.containsKey("$or"));
			assertFalse(filter.containsKey("$and"));

			Document prefixedFilter = MongodbMergeOperate.documentFilterForArrayNode(new Document("enroll_id", 1001),
					arrayNodeProperties("ENROLLMENT", Arrays.asList(joinKey("enroll_id", "ENROLLMENT.enroll_id"))));
			assertFalse(prefixedFilter.containsKey("$or"));
			assertEquals(new Document("ENROLLMENT.enroll_id", 1001), prefixedFilter);
		}

		@Test
		@DisplayName("without arrayPath or join keys the previous behaviour (empty filter) is kept")
		void testWithoutArrayPathOrJoinKeys() {
			assertTrue(MongodbMergeOperate.documentFilterForArrayNode(new Document("enroll_id", 1001),
					arrayNodeProperties(null, Arrays.asList(joinKey("enroll_id", "enroll_id")))).isEmpty());
			MergeTableProperties properties = new MergeTableProperties();
			properties.setArrayPath("ENROLLMENT");
			assertTrue(MongodbMergeOperate.documentFilterForArrayNode(new Document("enroll_id", 1001), properties).isEmpty());
		}

		@Test
		@DisplayName("TAP-12967: a duplicated join key target keeps the last value, exactly like arrayFilter does")
		void testDuplicatedTarget() {
			// the 3 arg arrayFilter() puts every join key into the same Document, so the last value wins.
			// Emitting $and:[{p:1},{p:2}] instead would require both values on the same array element, i.e. a
			// strict subset of the documents arrayFilters can update -> the update would silently be lost.
			Document filter = MongodbMergeOperate.documentFilterForArrayNode(
					new Document("a", 1).append("b", 2),
					arrayNodeProperties("ENROLLMENT",
							Arrays.asList(joinKey("a", "ENROLLMENT.p"), joinKey("b", "ENROLLMENT.p"))));

			assertFalse(filter.containsKey("$and"));
			assertEquals(new Document("ENROLLMENT.p", 2), filter);
		}

		@Test
		@DisplayName("TAP-12967: targets which differ raw but strip to the same element key keep the last value, they must not become an $and")
		void testSameElementKeyDifferentRawTarget() {
			// "ENROLLMENT.p" and "p" are two different raw targets of the same element field "p".
			// $and:[{...p:1},{...p:2}] would require both values on the same array element, i.e. a strict
			// subset of the documents arrayFilters can update -> the update would silently be lost.
			Document filter = MongodbMergeOperate.documentFilterForArrayNode(
					new Document("a", 1).append("b", 2),
					arrayNodeProperties("ENROLLMENT",
							Arrays.asList(joinKey("a", "ENROLLMENT.p"), joinKey("b", "p"))));

			assertFalse(filter.containsKey("$and"));
			assertFalse(filter.containsKey("$or"));
			assertEquals(1, filter.size());
			assertEquals(new Document("ENROLLMENT.p", 2), filter);
		}
	}

	@Nested
	@DisplayName("Method getArrayMatchString Test (through updateIntoArrayMerge)")
	class getArrayMatchStringTest {

		@Test
		@DisplayName("the arrayPath prefix must end at a '.' boundary and target equals arrayPath must not throw")
		void testPrefixBoundary() {
			MergeTableProperties properties = arrayNodeProperties("ENROLL", Arrays.asList(joinKey("v", "ENROLLMENT_X.y")));
			properties.setTargetPath("ENROLL.COURSE");
			properties.setArrayKeys(new ArrayList<>(Collections.singletonList("v")));
			MergeBundle mergeBundle = new MergeBundle(MergeBundle.EventOperation.INSERT, null, new Document("v", 1));
			MergeResult mergeResult = new MergeResult();

			MongodbMergeOperate.updateIntoArrayMerge(mergeBundle, properties, mergeResult, new MergeFilter(true));
			assertFalse(mergeResult.getFilter().isEmpty());
			assertEquals("{\"element1.ENROLLMENT_X.y\": 1}", firstArrayFilterJson(mergeResult));
			// the colliding prefix must not be stripped and must not be doubled: the element key is kept as is
			assertEquals("{\"ENROLL.ENROLLMENT_X.y\": 1}", mergeResult.getFilter().toJson());

			MergeTableProperties samePathProperties = arrayNodeProperties("ENROLL", Arrays.asList(joinKey("v", "ENROLL")));
			samePathProperties.setTargetPath("ENROLL");
			samePathProperties.setArrayKeys(new ArrayList<>(Collections.singletonList("v")));
			MergeResult samePathResult = new MergeResult();
			assertDoesNotThrow(() -> MongodbMergeOperate.updateIntoArrayMerge(mergeBundle, samePathProperties, samePathResult, new MergeFilter(true)));
			assertEquals("{\"element1.ENROLL\": 1}", firstArrayFilterJson(samePathResult));
			// target equals arrayPath: neither stripped (that would leave an empty key) nor thrown, kept as is
			assertEquals("{\"ENROLL.ENROLL\": 1}", samePathResult.getFilter().toJson());
		}
	}

	private static Map<String, String> joinKey(String source, String target) {
		Map<String, String> joinKey = new HashMap<>();
		joinKey.put("source", source);
		joinKey.put("target", target);
		return joinKey;
	}

	@SafeVarargs
	private static MergeTableProperties arrayNodeProperties(String arrayPath, List<Map<String, String>>... joinKeys) {
		MergeTableProperties properties = new MergeTableProperties();
		properties.setMergeType(MergeTableProperties.MergeType.updateIntoArray);
		properties.setIsArray(true);
		properties.setArrayPath(arrayPath);
		List<Map<String, String>> allJoinKeys = new ArrayList<>();
		for (List<Map<String, String>> joinKey : joinKeys) {
			allJoinKeys.addAll(joinKey);
		}
		properties.setJoinKeys(allJoinKeys);
		return properties;
	}

	private static String firstArrayFilterJson(MergeResult mergeResult) {
		return mergeResult.getUpdateOptions().getArrayFilters().get(0).toBsonDocument().toJson();
	}
}
