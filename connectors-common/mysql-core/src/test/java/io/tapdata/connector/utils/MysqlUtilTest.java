package io.tapdata.connector.utils;

import io.tapdata.connector.mysql.util.MysqlUtil;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapTimeValue;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author samuel
 * @Description
 * @create 2022-07-29 12:11
 **/
public class MysqlUtilTest {
	@Test
	public void fixDatatypeTest() {
		String datatype = "datetime(0)";
		String version = "5.5.6-log";
		datatype = MysqlUtil.fixDataType(datatype, version);
		Assertions.assertEquals("datetime", datatype);
		datatype = "timestamp(6)";
		datatype = MysqlUtil.fixDataType(datatype, version);
		Assertions.assertEquals("timestamp", datatype);
		datatype = "TIMESTAMP(6)";
		datatype = MysqlUtil.fixDataType(datatype, version);
		Assertions.assertEquals("TIMESTAMP", datatype);
	}

	@Test
	public void object2StringPreservesNegativeZeroTime() {
		Assertions.assertEquals("'-00:00:00.000001'",
				MysqlUtil.object2String(new TapTimeValue(DateTime.withTimeStr("-00:00:00.000001"))));
		Assertions.assertEquals("'-12:34:56.123456'",
				MysqlUtil.object2String(new TapTimeValue(DateTime.withTimeStr("-12:34:56.123456"))));
	}
}
