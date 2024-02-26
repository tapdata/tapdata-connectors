package io.tapdata.dummy.utils;

import io.tapdata.dummy.constants.SyncStage;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author samuel
 * @Description
 * @create 2024-02-23 18:53
 **/
@DisplayName("TapEventBuilder class test")
public class TapEventBuilderTest {

	private TapEventBuilder tapEventBuilder;

	@BeforeEach
	void setUp() {
		tapEventBuilder = new TapEventBuilder();
	}

	@Nested
	@DisplayName("Method generateInsertRecordEvent test")
	class generateInsertRecordEventTest{

		@BeforeEach
		void setUp() {
			tapEventBuilder.reset(null, SyncStage.Initial);
		}

		@Test
		@DisplayName("Test generate insert record event: uuid type")
		void testUUID() {
			TapTable table = new TapTable("dummy_test")
					.add(new TapField("uuid", "uuid"));

			TapInsertRecordEvent tapInsertRecordEvent = tapEventBuilder.generateInsertRecordEvent(table);

			assertNotNull(tapInsertRecordEvent);
			Map<String, Object> after = tapInsertRecordEvent.getAfter();
			assertNotNull(after);
			assertEquals(table.getNameFieldMap().size(), after.size());
			assertTrue(after.containsKey("uuid"));
			assertNotNull(after.get("uuid"));
			assertInstanceOf(String.class, after.get("uuid"));
		}

		@Test
		@DisplayName("Test generate insert record event: now type")
		void testNow() {
			TapTable table = new TapTable("dummy_test")
					.add(new TapField("created", "now"));

			TapInsertRecordEvent tapInsertRecordEvent = tapEventBuilder.generateInsertRecordEvent(table);

			assertNotNull(tapInsertRecordEvent);
			Map<String, Object> after = tapInsertRecordEvent.getAfter();
			assertNotNull(after);
			assertEquals(table.getNameFieldMap().size(), after.size());
			assertTrue(after.containsKey("created"));
			assertNotNull(after.get("created"));
			assertInstanceOf(Timestamp.class, after.get("created"));
		}

		@Test
		@DisplayName("Test generate insert record event: rnumber type")
		void testRNumber() {
			TapTable table = new TapTable("dummy_test")
					.add(new TapField("id", "rnumber"))
					.add(new TapField("num1", "rnumber(10)"));

			TapInsertRecordEvent tapInsertRecordEvent = tapEventBuilder.generateInsertRecordEvent(table);

			assertNotNull(tapInsertRecordEvent);
			Map<String, Object> after = tapInsertRecordEvent.getAfter();
			assertNotNull(after);
			assertEquals(table.getNameFieldMap().size(), after.size());
			assertTrue(after.containsKey("id"));
			assertNotNull(after.get("id"));
			assertInstanceOf(Double.class, after.get("id"));
			assertTrue(after.containsKey("num1"));
			assertNotNull(after.get("num1"));
			assertInstanceOf(Double.class, after.get("num1"));
		}

		@Test
		@DisplayName("Test generate insert record event: rstring type")
		void testRString() {
			TapTable table = new TapTable("dummy_test")
					.add(new TapField("str", "rstring"))
					.add(new TapField("str1", "rstring(1)"));

			TapInsertRecordEvent tapInsertRecordEvent = tapEventBuilder.generateInsertRecordEvent(table);

			assertNotNull(tapInsertRecordEvent);
			Map<String, Object> after = tapInsertRecordEvent.getAfter();
			assertNotNull(after);
			assertEquals(table.getNameFieldMap().size(), after.size());
			assertTrue(after.containsKey("str"));
			assertNotNull(after.get("str"));
			assertInstanceOf(String.class, after.get("str"));
			assertTrue(after.containsKey("str1"));
			assertNotNull(after.get("str1"));
			assertInstanceOf(String.class, after.get("str1"));
		}

		@Test
		@DisplayName("Test generate insert record event: serial type")
		void testSerialDefault() {
			TapTable table = new TapTable("dummy_test")
					.add(new TapField("id", "serial"));

			TapInsertRecordEvent tapInsertRecordEvent = tapEventBuilder.generateInsertRecordEvent(table);

			assertNotNull(tapInsertRecordEvent);
			Map<String, Object> after = tapInsertRecordEvent.getAfter();
			assertNotNull(after);
			assertEquals(table.getNameFieldMap().size(), after.size());
			assertTrue(after.containsKey("id"));
			assertNotNull(after.get("id"));
			assertInstanceOf(Long.class, after.get("id"));
			assertEquals(1L, after.get("id"));

			tapInsertRecordEvent = tapEventBuilder.generateInsertRecordEvent(table);
			assertNotNull(tapInsertRecordEvent);
			after = tapInsertRecordEvent.getAfter();
			assertNotNull(after);
			assertEquals(table.getNameFieldMap().size(), after.size());
			assertTrue(after.containsKey("id"));
			assertNotNull(after.get("id"));
			assertInstanceOf(Long.class, after.get("id"));
			assertEquals(2L, after.get("id"));
		}

		@Test
		@DisplayName("Test generate insert record event: serial(8,2) type")
		void testSerialStart8Step2() {
			TapTable table = new TapTable("dummy_test")
					.add(new TapField("serial_8_2", "serial(8,2)"))
					.add(new TapField("serial_8_2_2", "serial"));

			TapInsertRecordEvent tapInsertRecordEvent = tapEventBuilder.generateInsertRecordEvent(table);

			assertNotNull(tapInsertRecordEvent);
			Map<String, Object> after = tapInsertRecordEvent.getAfter();
			assertNotNull(after);
			assertEquals(table.getNameFieldMap().size(), after.size());
			assertTrue(after.containsKey("serial_8_2"));
			assertNotNull(after.get("serial_8_2"));
			assertInstanceOf(Long.class, after.get("serial_8_2"));
			assertEquals(8L, after.get("serial_8_2"));
			assertTrue(after.containsKey("serial_8_2_2"));
			assertNotNull(after.get("serial_8_2_2"));
			assertInstanceOf(Long.class, after.get("serial_8_2_2"));
			assertEquals(10L, after.get("serial_8_2_2"));
		}

		@Test
		@DisplayName("Test generate insert record event: two serial in one table")
		void testTwoSerialInOneTable() {
			TapTable table = new TapTable("dummy_test")
					.add(new TapField("id", "serial"))
					.add(new TapField("id1", "serial"));

			TapInsertRecordEvent tapInsertRecordEvent = tapEventBuilder.generateInsertRecordEvent(table);

			assertNotNull(tapInsertRecordEvent);
			Map<String, Object> after = tapInsertRecordEvent.getAfter();
			assertNotNull(after);
			assertEquals(table.getNameFieldMap().size(), after.size());
			assertTrue(after.containsKey("id"));
			assertNotNull(after.get("id"));
			assertInstanceOf(Long.class, after.get("id"));
			assertEquals(1L, after.get("id"));
			assertTrue(after.containsKey("id1"));
			assertNotNull(after.get("id1"));
			assertInstanceOf(Long.class, after.get("id1"));
			assertEquals(2L, after.get("id1"));
		}

		@Test
		@DisplayName("Test generate insert record event: string type default 'default title'")
		void testDefaultValue() {
			String defaultValue = "default title";
			TapTable table = new TapTable("dummy_test")
					.add(new TapField("title", "string").defaultValue(defaultValue));

			TapInsertRecordEvent tapInsertRecordEvent = tapEventBuilder.generateInsertRecordEvent(table);

			assertNotNull(tapInsertRecordEvent);
			Map<String, Object> after = tapInsertRecordEvent.getAfter();
			assertNotNull(after);
			assertEquals(table.getNameFieldMap().size(), after.size());
			assertTrue(after.containsKey("title"));
			assertNotNull(after.get("title"));
			assertEquals(defaultValue, after.get("title"));
		}
	}
}
