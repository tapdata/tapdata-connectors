package io.tapdata.mongodb.writer;

import io.tapdata.mongodb.entity.MergeFilter;
import io.tapdata.mongodb.entity.MergeResult;
import io.tapdata.mongodb.merge.MergeFilterManager;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
}