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
	void testPlainFilterUsesDefaultLimit() {
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
	void testEmptyObjectMatchesEverything() {
		MongodbRawCommandQuery query = MongodbRawCommandQuery.parse("{}", 100);

		assertTrue(query.getFilter().isEmpty());
		assertTrue(query.getSort().isEmpty());
		assertEquals(100, query.getLimit());
	}

	@Test
	void testWrappedCommand() {
		MongodbRawCommandQuery query = MongodbRawCommandQuery.parse(
				"{\"$find\":{\"filter\":{\"age\":{\"$gt\":18}},\"sort\":{\"age\":-1},\"limit\":10}}", 100);

		assertEquals(new Document("age", new Document("$gt", 18)), query.getFilter());
		assertEquals(new Document("age", -1), query.getSort());
		assertEquals(10, query.getLimit());
	}

	@Test
	void testWrappedCommandWithoutLimitUsesDefaultLimit() {
		MongodbRawCommandQuery query = MongodbRawCommandQuery.parse("{\"$find\":{\"filter\":{\"a\":1}}}", 100);

		assertEquals(new Document("a", 1), query.getFilter());
		assertEquals(100, query.getLimit());
	}

	@Test
	void testWrappedFilterAsJsonString() {
		MongodbRawCommandQuery query = MongodbRawCommandQuery.parse("{\"$find\":{\"filter\":\"{\\\"a\\\":1}\"}}", 100);

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
	void testWrappedLimitOverridesDefaultLimit() {
		assertEquals(10, MongodbRawCommandQuery.parse("{\"$find\":{\"limit\":10}}", 100).getLimit());
		assertEquals(10000, MongodbRawCommandQuery.parse("{\"$find\":{\"limit\":10000}}", 100).getLimit());
		assertEquals(Integer.MAX_VALUE, MongodbRawCommandQuery.parse("{\"$find\":{\"limit\":10000000000}}", 100).getLimit());
	}

	@Test
	void testNonPositiveOrNonNumericLimitIsRejected() {
		for (String limit : Arrays.asList("0", "-5", "\"10\"", "{}")) {
			String command = "{\"$find\":{\"limit\":" + limit + "}}";
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> MongodbRawCommandQuery.parse(command, 50), command);
			assertTrue(e.getMessage().contains("limit"), e.getMessage());
		}
	}

	@Test
	void testNonPositiveDefaultLimitUsesBuiltInDefault() {
		assertEquals(MongodbRawCommandQuery.DEFAULT_LIMIT, MongodbRawCommandQuery.parse("{}", 0).getLimit());
	}

	/**
	 * A field named like one of the wrapped keys is a plain filter, no matter whether other fields are present. See
	 * {@link MongodbRawCommandQuery} for why {@code $find} is the only marker that switches to the wrapped form.
	 */
	@Test
	void testReservedKeyAloneIsTreatedAsFilter() {
		MongodbRawCommandQuery limitAsField = MongodbRawCommandQuery.parse("{\"limit\":5}", 100);
		assertEquals(new Document("limit", 5), limitAsField.getFilter());
		assertTrue(limitAsField.getSort().isEmpty());
		assertEquals(100, limitAsField.getLimit());

		MongodbRawCommandQuery filterAsField = MongodbRawCommandQuery.parse("{\"filter\":{\"status\":\"PAID\"}}", 100);
		assertEquals(new Document("filter", new Document("status", "PAID")), filterAsField.getFilter());
		assertEquals(100, filterAsField.getLimit());

		MongodbRawCommandQuery sortAsField = MongodbRawCommandQuery.parse("{\"sort\":{\"age\":1}}", 100);
		assertEquals(new Document("sort", new Document("age", 1)), sortAsField.getFilter());
		assertTrue(sortAsField.getSort().isEmpty());
		assertEquals(100, sortAsField.getLimit());
	}

	@Test
	void testReservedKeyMixedWithFieldIsTreatedAsFilter() {
		MongodbRawCommandQuery query = MongodbRawCommandQuery.parse("{\"limit\":5,\"age\":1}", 100);

		assertEquals(new Document("limit", 5).append("age", 1), query.getFilter());
		assertEquals(100, query.getLimit());
	}

	@Test
	void testWrapperMixedWithOtherKeysIsRejected() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> MongodbRawCommandQuery.parse("{\"$find\":{\"filter\":{}},\"age\":1}", 100));
		assertTrue(e.getMessage().contains("$find"), e.getMessage());
	}

	@Test
	void testUnknownWrappedKeyIsRejected() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> MongodbRawCommandQuery.parse("{\"$find\":{\"filter\":{},\"projection\":{\"a\":1}}}", 100));
		assertTrue(e.getMessage().contains("projection"), e.getMessage());
	}

	@Test
	void testNonObjectFilterIsRejected() {
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
				() -> MongodbRawCommandQuery.parse("{\"$find\":{\"filter\":[1,2]}}", 100));
		assertTrue(e.getMessage().contains("filter"), e.getMessage());
	}

	@Test
	void testInvalidJsonIsRejected() {
		assertThrows(IllegalArgumentException.class, () -> MongodbRawCommandQuery.parse("db.orders.find({})", 100));
		assertThrows(IllegalArgumentException.class, () -> MongodbRawCommandQuery.parse("[1,2]", 100));
	}

	@Test
	void testServerSideJavaScriptIsRejected() {
		for (String command : Arrays.asList(
				"{\"$where\":\"this.a == 1\"}",
				"{\"$and\":[{\"$where\":\"this.a == 1\"}]}",
				"{\"a\":{\"$expr\":{\"$function\":{\"body\":\"function(){return true}\",\"args\":[],\"lang\":\"js\"}}}}",
				"{\"$find\":{\"filter\":{\"$where\":\"this.a == 1\"}}}")) {
			IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
					() -> MongodbRawCommandQuery.parse(command, 100), command);
			assertTrue(e.getMessage().contains("JavaScript"), e.getMessage());
		}
	}

	@Test
	void testQueryOperatorsAreAccepted() {
		MongodbRawCommandQuery query = MongodbRawCommandQuery.parse(
				"{\"$or\":[{\"age\":{\"$gte\":18}},{\"$expr\":{\"$eq\":[\"$a\",\"$b\"]}}]}", 100);

		assertEquals(2, ((java.util.List<?>) query.getFilter().get("$or")).size());
	}
}
