package io.tapdata.connector.tidb.cdc.process.ddl.convert;

import io.tapdata.entity.error.CoreException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

class ConvertTest {
    Convert convert;

    @BeforeEach
    void setUp() {
        convert = new Convert() {
            @Override
            public Object convert(Object fromValue) {
                return null;
            }
        };
    }

    @Test
    void testParseInt() {
        Assertions.assertEquals(1, convert.parseInt("1", 1));
        Assertions.assertEquals(1, convert.parseInt("x", 1));
    }

    @Test
    void testCovertToDateTime() {
        Object c = convert.covertToDateTime("2024-06-11 16:33:00.0", 1, "yyyy-MM-dd HH:mm:ss", TimeZone.getDefault());
        Assertions.assertNotNull(c);
        Assertions.assertEquals(LocalDateTime.class.getName(), c.getClass().getName());

        c = convert.covertToDateTime(System.currentTimeMillis(), 0, "yyyy-MM-dd", TimeZone.getDefault());
        Assertions.assertNotNull(c);
        Assertions.assertEquals(Long.class.getName(), c.getClass().getName());

        c = convert.covertToDateTime(System.currentTimeMillis(), -1, "yyyy-MM-dd", TimeZone.getDefault());
        Assertions.assertNotNull(c);
        Assertions.assertEquals(Long.class.getName(), c.getClass().getName());

        c = convert.covertToDateTime(System.currentTimeMillis(), 1, "yyyy-MM-dd", TimeZone.getDefault());
        Assertions.assertNotNull(c);
        Assertions.assertEquals(Long.class.getName(), c.getClass().getName());

        Assertions.assertThrows(CoreException.class, () -> {
            convert.covertToDateTime("2024-06-11 16:33:00.0", 0, "hthfsyyyy-MM-dd", TimeZone.getDefault());
        });
        Assertions.assertThrows(CoreException.class, () -> {
            convert.covertToDateTime("2024-06-11 16:33:00.0", 1, "hthfsyyyy-MM-dd", TimeZone.getDefault());
        });
    }

    @Test
    void testCovertToDate() {
        Object c = convert.covertToDate("2024-06-11", 0, "yyyy-MM-dd", TimeZone.getDefault());
        Assertions.assertNotNull(c);
        Assertions.assertEquals(LocalDate.class.getName(), c.getClass().getName());

        c = convert.covertToDate(System.currentTimeMillis(), 0, "yyyy-MM-dd", TimeZone.getDefault());
        Assertions.assertNotNull(c);
        Assertions.assertEquals(Long.class.getName(), c.getClass().getName());

        Assertions.assertNotNull(convert.covertToDate("2024-06-11", 0, "yyyy-MM-dd", TimeZone.getDefault()));
    }

    @Test
    void testInstance() {
        testOne("CHAR", "1", "1", CharConvert.class);
        testOne("VARCHAR", "1", "1", VarCharConvert.class);
        testOne("TINYTEXT", "1", "1", VarCharConvert.class);
        testOne("TEXT", "1", "1", VarCharConvert.class);
        testOne("MEDIUMTEXT", "1", "1", VarCharConvert.class);
        testOne("LONGTEXT", "1", "1", VarCharConvert.class);
        testOne("JSON", "1", "1", VarCharConvert.class);
        testOne("BINARY", "1", "1", BinaryConvert.class);
        testOne("VARBINARY", "1", "1", BinaryConvert.class);
        testOne("TINYBLOB", "1", "1", BinaryConvert.class);
        testOne("BLOB", "1", "1", BinaryConvert.class);
        testOne("MEDIUMBLOB", "1", "1", BinaryConvert.class);
        testOne("LONGBLOB", "1", "1", BinaryConvert.class);
        testOne("BIT UNSIGNED", "1", "1", BitConvert.class);
        testOne("BIT", "1", "1", BitConvert.class);
        testOne("TINYINT UNSIGNED", "1", "1", TinyIntConvert.class);
        testOne("TINYINT", "1", "1", TinyIntConvert.class);
        testOne("SMALLINT", "1", "1", SmallIntConvert.class);
        testOne("SMALLINT UNSIGNED", "1", "1", SmallIntConvert.class);
        testOne("INT UNSIGNED", "1", "1", IntegerConvert.class);
        testOne("MEDIUMINT UNSIGNED", "1", "1", IntegerConvert.class);
        testOne("INT", "1", "1", IntegerConvert.class);
        testOne("MEDIUMINT", "1", "1", IntegerConvert.class);
        testOne("BIGINT UNSIGNED", "1", "1", LongConvert.class);
        testOne("BIGINT", "1", "1", LongConvert.class);
        testOne("DECIMAL", "1", "1", DecimalConvert.class);
        testOne("FLOAT", "1", "1", FloatConvert.class);
        testOne("FLOAT UNSIGNED", "1", "1", FloatConvert.class);
        testOne("DOUBLE", "1", "1", DoubleConvert.class);
        testOne("DOUBLE UNSIGNED", "1", "1", DoubleConvert.class);
        testOne("TIMESTAMP", "1", "1", TimestampConvert.class);
        testOne("DATETIME", "1", "1", DateTimeConvert.class);
        testOne("TIME", "1", "1", TimeConvert.class);
        testOne("DATE", "1", "1", DateConvert.class);
        testOne("YEAR UNSIGNED", "1", "1", YearConvert.class);
        testOne("YEAR", "1", "1", YearConvert.class);
        testOne("ENUM", "1", "1", EnumConvert.class);
        testOne("SET", "1", "1", SetConvert.class);
        testOne("**", "1", "1", DefaultConvert.class);
    }

    void testOne(String columnType, String columnPrecision, String columnScale, Class<? extends Convert> c) {
        Map<String, Object> convertInfo = new HashMap<>();
        convertInfo.put(Convert.COLUMN_TYPE, columnType);
        convertInfo.put(Convert.COLUMN_PRECISION, columnPrecision);
        convertInfo.put(Convert.COLUMN_SCALE, columnScale);
        Convert instance = Convert.instance(convertInfo, TimeZone.getDefault());
        Assertions.assertNotNull(instance);
        Assertions.assertEquals(c.getName(), instance.getClass().getName());
    }
}