package io.tapdata.mongodb;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MongodbRawCommandQueryTest {

	@Test
	void testPlainFilterUsesMaxRows() {
		MongodbRawCommandQuery query = MongodbRawCommandQuery.parse("{\"status\":\"PAID\"}", 100);

		assertEquals(new Document("status", "PAID"), query.getFilter());
		assertTrue(query.getSort().isEmpty());
		assertEquals(100, query.getLimit());
	}

	@Test
	void testBlankCommandIsEmptyFilter() {
		for (String command : Arrays.asList(null, "", "   ")) {
			MongodbRawCommandQuery query = MongodbRawCommandQuery.parse(command, 100);

			assertTrue(query.getFilter().isEmpty(), String.valueOf(command));
			assertEquals(100, query.getLimit());
		}
	}

	@Test
	void testWrappedCommand() {
		MongodbRawCommandQuery query = MongodbRawCommandQuery.parse(
				"{\"filter\":{\"age\":{\"$gt\":18}},\"sort\":{\"age\":-1},\"limit\":10}", 100);

		assertEquals(new Document("age", new Document("$gt", 18)), query.getFilter());
		assertEquals(new Document("age", -1), query.getSort());
		assertEquals(10, query.getLimit());
	}

	@Test
	void testWrappedFilterAsJsonString() {
		MongodbRawCommandQuery query = MongodbRawCommandQuery.parse("{\"filter\":\"{\\\"a\\\":1}\"}", 100);

		assertEquals(new Document("a", 1), query.getFilter());
	}

	@Test
	void testExtendedJsonTypesArePreserved() {
		MongodbRawCommandQuery query = MongodbRawCommandQuery.parse(
				"{\"_id\":{\"$oid\":\"5f1b2c3d4e5f6a7b8c9d0e1f\"},\"n\":{\"$numberLong\":\"9007199254740993\"}}", 100);

		assertInstanceOf(ObjectId.class, query.getFilter().get("_id"));
		assertEquals(9007199254740993L, query.getFilter().get("n"));
	}

	@Test
	void testLimitIsCappedByMaxRows() {
		assertEquals(100, MongodbRawCommandQuery.parse("{\"filter\":{},\"limit\":10000}", 100).getLimit());
		assertEquals(100, MongodbRawCommandQuery.parse("{\"limit\":10000000000}", 100).getLimit());
	}

	@Test
	void testNonPositiveOrNonNumericLimitUsesMaxRows() {
		assertEquals(50, MongodbRawCommandQuery.parse("{\"limit\":0}", 50).getLimit());
		assertEquals(50, MongodbRawCommandQuery.parse("{\"limit\":-5}", 50).getLimit());
		assertEquals(50, MongodbRawCommandQuery.parse("{\"limit\":\"10\"}", 50).getLimit());
	}

	@Test
	void testNonPositiveMaxRowsUsesDefault() {
		assertEquals(MongodbRawCommandQuery.DEFAULT_LIMIT, MongodbRawCommandQuery.parse("{}", 0).getLimit());
	}

	@Test
	void testReservedKeyMixedWithFieldIsTreatedAsFilter() {
		MongodbRawCommandQuery query = MongodbRawCommandQuery.parse("{\"limit\":5,\"age\":1}", 100);

		assertEquals(new Document("limit", 5).append("age", 1), query.getFilter());
		assertEquals(100, query.getLimit());
	}

	@Test
	void testNonObjectFilterIsRejected() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> MongodbRawCommandQuery.parse("{\"filter\":[1,2]}", 100));
		assertTrue(e.getMessage().contains("filter"));
	}

	@Test
	void testInvalidJsonIsRejected() {
		assertThrows(IllegalArgumentException.class, () -> MongodbRawCommandQuery.parse("db.orders.find({})", 100));
		assertThrows(IllegalArgumentException.class, () -> MongodbRawCommandQuery.parse("[1,2]", 100));
	}
}
