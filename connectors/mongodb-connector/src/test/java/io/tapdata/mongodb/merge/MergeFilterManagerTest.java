package io.tapdata.mongodb.merge;

import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author samuel
 * @Description
 * @create 2024-04-23 15:58
 **/
@DisplayName("Class MergeFilterManager Test")
class MergeFilterManagerTest {
	@Test
	@DisplayName("Test main process")
	void testMainProcess() {
		Map.Entry<String, Object> entry = new Document("id", 1).entrySet().iterator().next();

		assertFalse(MergeFilterManager.test(entry));
		assertTrue(MergeFilterManager.test(null));
	}

	@Test
	@DisplayName("Test entry value have $not->$elemMatch")
	void testHaveNotElemMatch() {
		List<Document> elemMatchList = new ArrayList<>();
		elemMatchList.add(new Document("id", 1));
		Document elemMatch = new Document("$elemMatch", elemMatchList);
		Document not = new Document("$not", elemMatch);
		Document document = new Document("arr", not).append("id", 1);

		Iterator<Map.Entry<String, Object>> iterator = document.entrySet().iterator();

		assertTrue(MergeFilterManager.test(iterator.next()));
		assertFalse(MergeFilterManager.test(iterator.next()));
	}
}