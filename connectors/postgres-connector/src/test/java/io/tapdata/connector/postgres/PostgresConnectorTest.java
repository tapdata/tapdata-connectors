package io.tapdata.connector.postgres;

import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.JdbcContext;
import io.tapdata.connector.postgres.PostgresConnector;
import io.tapdata.connector.postgres.PostgresSqlMaker;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.*;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.common.vo.TapHashResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

public class PostgresConnectorTest {

    @Test
    void testRegisterCapabilitiesQueryTableHash(){
        PostgresConnector postgresConnector = new PostgresConnector();
        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
        ReflectionTestUtils.invokeMethod(postgresConnector,"registerCapabilities",connectorFunctions,codecRegistry);
        Assertions.assertTrue(connectorFunctions.getQueryHashByAdvanceFilterFunction()!=null);
    }


    @Test
    void testQueryTableHash() throws SQLException {

        PostgresConnector postgresConnector = new PostgresConnector();
        TapConnectorContext connectorContext = Mockito.mock(TapConnectorContext.class);
        TapAdvanceFilter filter = new TapAdvanceFilter();
        TapTable table = new TapTable();
        LinkedHashMap<String, TapField> map = new LinkedHashMap<>();
        table.setNameFieldMap(map);
        JdbcContext jdbcContext = Mockito.mock(JdbcContext.class);
        ReflectionTestUtils.setField(postgresConnector,"jdbcContext",jdbcContext);
        CommonSqlMaker commonSqlMaker = new PostgresSqlMaker();
        ReflectionTestUtils.setField(postgresConnector,"commonSqlMaker",commonSqlMaker);
        doNothing().when(jdbcContext).query(Mockito.anyString(),Mockito.any());

        Consumer<TapHashResult<String>> consumer = new Consumer<TapHashResult<String>>() {
            @Override
            public void accept(TapHashResult<String> stringTapHashResult) {
                Assertions.assertTrue(stringTapHashResult == null);
            }
        };
        ReflectionTestUtils.invokeMethod(postgresConnector,"queryTableHash",connectorContext,filter,table,consumer);

    }


    @Test
    void testBuildHashSql() throws SQLException {

        PostgresConnector postgresConnector = new PostgresConnector();
        TapAdvanceFilter filter = new TapAdvanceFilter();
        TapTable table = new TapTable();
        LinkedHashMap<String, TapField> map = new LinkedHashMap<>();

        buildNumberTapField("real",map);
        buildNumberTapField("double",map);
        buildNumberTapField("numeric",map);
        buildNumberTapField("float",map);


        TapField stringTapField  = new TapField();
        stringTapField.setTapType(new TapString());
        stringTapField.setName("character");
        stringTapField.setDataType("character(6)");
        map.put("character",stringTapField);


        TapField booleanTapField  = new TapField();
        booleanTapField.setTapType(new TapBoolean());
        booleanTapField.setName("boolean");
        booleanTapField.setDataType("boolean");
        map.put("boolean",booleanTapField);

        TapField timeTapField  = new TapField();
        timeTapField.setTapType(new TapTime());
        timeTapField.setName("time");
        timeTapField.setDataType("time with time zone");
        map.put("time",timeTapField);

        TapField datetimeTapField  = new TapField();
        datetimeTapField.setTapType(new TapDateTime());
        datetimeTapField.setName("timestamp");
        datetimeTapField.setDataType("timestamp");
        map.put("timestamp",datetimeTapField);

        TapField binaryTapField  = new TapField();
        binaryTapField.setTapType(new TapBinary());
        binaryTapField.setName("bytea");
        binaryTapField.setDataType("bytea");
        map.put("bytea",binaryTapField);

        TapField intTapField  = new TapField();
        intTapField.setTapType(new TapNumber());
        intTapField.setName("bigint");
        intTapField.setDataType("bigint");
        map.put("int",intTapField);

        table.setNameFieldMap(map);
        JdbcContext jdbcContext = Mockito.mock(JdbcContext.class);
        ReflectionTestUtils.setField(postgresConnector,"jdbcContext",jdbcContext);
        CommonSqlMaker commonSqlMaker = new PostgresSqlMaker();
        ReflectionTestUtils.setField(postgresConnector,"commonSqlMaker",commonSqlMaker);

        String actualData =ReflectionTestUtils.invokeMethod(postgresConnector,"buildHashSql",filter,table);

        Assertions.assertTrue(actualData.contains("trunc(\"real\")"));
        Assertions.assertTrue(actualData.contains("trunc(\"double\")"));
        Assertions.assertTrue(actualData.contains("trunc(\"numeric\")"));
        Assertions.assertTrue(actualData.contains("trunc(\"float\")"));
        Assertions.assertTrue(actualData.contains("TRIM( \"character\" )"));
        Assertions.assertTrue(actualData.contains("CAST( \"boolean\" as int )"));
        Assertions.assertTrue(actualData.contains("SUBSTRING(cast(\"time\" as varchar) FROM 1 FOR 8)"));
        Assertions.assertTrue(!actualData.contains("bytea"));
        Assertions.assertTrue(actualData.contains("EXTRACT(epoch FROM CAST(date_trunc('second',\"timestamp\" ) AS TIMESTAMP))"));
        Assertions.assertTrue(actualData.contains("int"));


    }



    public void  buildNumberTapField(String name,LinkedHashMap<String, TapField> map){
        TapField numberTapField  = new TapField();
        numberTapField.setTapType(new TapNumber());
        numberTapField.setName(name);
        numberTapField.setDataType(name);
        map.put(name,numberTapField);
    }

    @Nested
    class GetHashSplitStringSqlTest {
        TapTable tapTable;
        PostgresConnector connector;

        @BeforeEach
        void setUp() {
            connector = mock(PostgresConnector.class);
            tapTable = mock(TapTable.class);
            doCallRealMethod().when(connector).getHashSplitStringSql(tapTable);
        }

        private LinkedHashMap<String, TapField> generateFieldMap(TapField... fields) {
            if (null == fields) {
                return null;
            }
            LinkedHashMap<String, TapField> fieldMap = new LinkedHashMap<>();
            for (TapField field : fields) {
                fieldMap.put(field.getName(), field);
            }
            return fieldMap;
        }

        @Test
        void testEmptyField() {
            // null getNameFieldMap
            doCallRealMethod().when(connector).getHashSplitStringSql(tapTable);
            assertThrows(CoreException.class, () -> connector.getHashSplitStringSql(tapTable));

            // empty getNameFieldMap
            when(tapTable.getNameFieldMap()).thenReturn(new LinkedHashMap<>());
            doCallRealMethod().when(connector).getHashSplitStringSql(tapTable);
            assertThrows(CoreException.class, () -> connector.getHashSplitStringSql(tapTable));
        }

        @Test
        void testNotPrimaryKeys() {
            LinkedHashMap<String, TapField> fieldMap = generateFieldMap(
                new TapField("ID", "INT"),
                new TapField("TITLE", "VARCHAR(64)")
            );
            when(tapTable.getNameFieldMap()).thenReturn(fieldMap);

            assertThrows(CoreException.class, () -> connector.getHashSplitStringSql(tapTable));
        }

        @Test
        void testTrue() {
            LinkedHashMap<String, TapField> fieldMap = generateFieldMap(
                new TapField("ID", "INT").primaryKeyPos(1),
                new TapField("TITLE", "VARCHAR(64)")
            );
            when(tapTable.getNameFieldMap()).thenReturn(fieldMap);

            assertNotNull(connector.getHashSplitStringSql(tapTable));
        }
    }
}
