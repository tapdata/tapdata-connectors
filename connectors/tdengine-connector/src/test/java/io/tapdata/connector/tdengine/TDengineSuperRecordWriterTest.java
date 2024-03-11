package io.tapdata.connector.tdengine;

import io.tapdata.connector.tdengine.config.TDengineConfig;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author samuel
 * @Description
 * @create 2024-02-27 15:41
 **/
@DisplayName("TDengineSuperRecordWriter class test")
class TDengineSuperRecordWriterTest {

	private TDengineJdbcContext tDengineJdbcContext;
	private TapTable tapTable;
	private TDengineSuperRecordWriter tDengineSuperRecordWriter;
	private TDengineConfig tDengineConfig;

	@BeforeEach
	void setUp() {
		tDengineConfig = new TDengineConfig();
		tDengineJdbcContext = mock(TDengineJdbcContext.class);
		when(tDengineJdbcContext.getConfig()).thenReturn(tDengineConfig);
		tapTable = new TapTable("test_table");
		tapTable.setNameFieldMap(new LinkedHashMap<>());
		tDengineSuperRecordWriter = new TDengineSuperRecordWriter(tDengineJdbcContext, tapTable);
	}

	@Nested
	@DisplayName("Method getSubTableName test")
	class getSubTableNameTest{
		@Test
		@DisplayName("Test get sub table name: other sub table name type")
		void testOtherSubTableNameType() {
			tDengineConfig.setSubTableNameType("Test");
			tDengineConfig.setSubTableSuffix(String.join("_", "${superTableName}", "${id}", "${name}"));
			Map<String, Object> after = new HashMap<>();
			after.put("id", 1);
			after.put("name", "test_name");
			after.put("created", new Date());
			TapInsertRecordEvent tapInsertRecordEvent = new TapInsertRecordEvent();
			tapInsertRecordEvent.setAfter(after);
			List<String> tags = new ArrayList<>();
			tags.add("id");
			tags.add("name");

			String subTableName = tDengineSuperRecordWriter.getSubTableName(tags, tapInsertRecordEvent);
			assertEquals("test_table_1_test_name", subTableName);
		}
	}

	@Nested
	@DisplayName("Method object2String test")
	class object2StringTest {
		@Test
		@DisplayName("Test object to string")
		void testString() {
			String str = String.join(",", "\\", "'", "(test)");
			String actual = tDengineSuperRecordWriter.object2String(str);
			assertEquals("'\\\\,\\',\\(test\\)'", actual);
		}
	}
}
