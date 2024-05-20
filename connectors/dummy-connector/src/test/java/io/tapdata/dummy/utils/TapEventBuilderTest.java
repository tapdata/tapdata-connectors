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
class TapEventBuilderTest {

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
					.add(new TapField("num", "rnumber"))
					.add(new TapField("num_1", "rnumber"))
					.add(new TapField("num10", "rnumber(10)"));

			TapInsertRecordEvent tapInsertRecordEvent = tapEventBuilder.generateInsertRecordEvent(table);

			assertNotNull(tapInsertRecordEvent);
			Map<String, Object> after = tapInsertRecordEvent.getAfter();
			assertNotNull(after);
			assertEquals(table.getNameFieldMap().size(), after.size());
			assertTrue(after.containsKey("num"));
			assertNotNull(after.get("num"));
			assertInstanceOf(Double.class, after.get("num"));
			assertTrue(after.containsKey("num_1"));
			assertNotNull(after.get("num_1"));
			assertInstanceOf(Double.class, after.get("num_1"));
			assertEquals(after.get("num"), after.get("num_1"));
			assertTrue(after.containsKey("num10"));
			assertNotNull(after.get("num10"));
			assertInstanceOf(Double.class, after.get("num10"));
		}

		@Test
		@DisplayName("Test generate insert record event: rstring type")
		void testRString() {
			TapTable table = new TapTable("dummy_test")
					.add(new TapField("str", "rstring"))
					.add(new TapField("str_1", "rstring"))
					.add(new TapField("str1", "rstring(1)"));

			TapInsertRecordEvent tapInsertRecordEvent = tapEventBuilder.generateInsertRecordEvent(table);

			assertNotNull(tapInsertRecordEvent);
			Map<String, Object> after = tapInsertRecordEvent.getAfter();
			assertNotNull(after);
			assertEquals(table.getNameFieldMap().size(), after.size());
			assertTrue(after.containsKey("str"));
			assertNotNull(after.get("str"));
			assertInstanceOf(String.class, after.get("str"));
			assertEquals(TapEventBuilder.DEFAULT_RANDOM_STRING_LENGTH, after.get("str").toString().length());
			assertTrue(after.containsKey("str_1"));
			assertNotNull(after.get("str_1"));
			assertInstanceOf(String.class, after.get("str_1"));
			assertEquals(after.get("str"), after.get("str_1"));
			assertTrue(after.containsKey("str1"));
			assertNotNull(after.get("str1"));
			assertInstanceOf(String.class, after.get("str1"));
			assertEquals(1, after.get("str1").toString().length());
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

		@Test
		@DisplayName("Test generate insert record event: rlongstring type")
		void testRLongString() {
			TapTable table = new TapTable("dummy_test")
					.add(new TapField("str", "rlongstring"))
					.add(new TapField("str_1", "rlongstring"))
					.add(new TapField("str1", "rlongstring(100)"));

			TapInsertRecordEvent tapInsertRecordEvent = tapEventBuilder.generateInsertRecordEvent(table);

			assertNotNull(tapInsertRecordEvent);
			Map<String, Object> after = tapInsertRecordEvent.getAfter();
			assertNotNull(after);
			assertEquals(table.getNameFieldMap().size(), after.size());
			assertTrue(after.containsKey("str"));
			assertNotNull(after.get("str"));
			assertInstanceOf(String.class, after.get("str"));
			assertEquals(TapEventBuilder.LONG_RANDOM_STRING_LENGTH, after.get("str").toString().length());
			assertTrue(after.containsKey("str_1"));
			assertNotNull(after.get("str_1"));
			assertInstanceOf(String.class, after.get("str_1"));
			assertEquals(after.get("str"), after.get("str_1"));
			assertTrue(after.containsKey("str1"));
			assertNotNull(after.get("str1"));
			assertInstanceOf(String.class, after.get("str1"));
			assertEquals(100, after.get("str1").toString().length());
		}

		@Test
		@DisplayName("Test generate insert record event: rlongbinary type")
		void testRLongBinary() {
			TapTable table = new TapTable("dummy_test")
					.add(new TapField("bin", "rlongbinary"))
					.add(new TapField("bin_1", "rlongbinary"))
					.add(new TapField("bin1", "rlongbinary(100)"));

			TapInsertRecordEvent tapInsertRecordEvent = tapEventBuilder.generateInsertRecordEvent(table);

			assertNotNull(tapInsertRecordEvent);
			Map<String, Object> after = tapInsertRecordEvent.getAfter();
			assertNotNull(after);
			assertEquals(table.getNameFieldMap().size(), after.size());
			assertTrue(after.containsKey("bin"));
			assertNotNull(after.get("bin"));
			assertInstanceOf(byte[].class, after.get("bin"));
			assertEquals(TapEventBuilder.LONG_RANDOM_STRING_LENGTH, ((byte[])after.get("bin")).length);
			assertTrue(after.containsKey("bin_1"));
			assertNotNull(after.get("bin_1"));
			assertInstanceOf(byte[].class, after.get("bin_1"));
			assertEquals(after.get("bin"), after.get("bin_1"));
			assertTrue(after.containsKey("bin1"));
			assertNotNull(after.get("bin1"));
			assertInstanceOf(byte[].class, after.get("bin1"));
			assertEquals(100, ((byte[])after.get("bin1")).length);
		}

		@Test
		@DisplayName("Test generate insert record event: rdatetime type")
		void testRDatetime() {
			TapTable table = new TapTable("dummy_test")
					.add(new TapField("date", "rdatetime"))
					.add(new TapField("date_1", "rdatetime"))
					.add(new TapField("date1", "rdatetime(3)"));

			TapInsertRecordEvent tapInsertRecordEvent = tapEventBuilder.generateInsertRecordEvent(table);

			assertNotNull(tapInsertRecordEvent);
			Map<String, Object> after = tapInsertRecordEvent.getAfter();
			assertNotNull(after);
			assertEquals(table.getNameFieldMap().size(), after.size());
			assertTrue(after.containsKey("date"));
			assertNotNull(after.get("date"));
			assertInstanceOf(Timestamp.class, after.get("date"));
			assertEquals(((Timestamp) after.get("date")).getNanos(), (long) ((long) ((Timestamp) after.get("date")).getNanos() / Math.pow(10, 9 - TapEventBuilder.DEFAULT_RANDOM_DATE_FRACTION)) * Math.pow(10, 9 - TapEventBuilder.DEFAULT_RANDOM_DATE_FRACTION));
			assertTrue(after.containsKey("date_1"));
			assertNotNull(after.get("date_1"));
			assertInstanceOf(Timestamp.class, after.get("date_1"));
			assertEquals(after.get("date"), after.get("date_1"));
			assertTrue(after.containsKey("date1"));
			assertNotNull(after.get("date1"));
			assertInstanceOf(Timestamp.class, after.get("date1"));
			assertEquals(((Timestamp) after.get("date1")).getNanos(), (long) ((long) ((Timestamp) after.get("date1")).getNanos() / Math.pow(10, 6)) * Math.pow(10, 6));
		}
	}
}
