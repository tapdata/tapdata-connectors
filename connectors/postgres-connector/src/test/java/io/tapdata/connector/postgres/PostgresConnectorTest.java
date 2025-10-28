package io.tapdata.connector.postgres;

import io.tapdata.common.CommonSqlMaker;
import io.tapdata.common.JdbcContext;
import io.tapdata.connector.postgres.partition.PostgresPartitionContext;
import io.tapdata.connector.postgres.partition.TableType;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.TapPartition;
import io.tapdata.entity.schema.type.*;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.common.vo.TapHashResult;
import io.tapdata.pdk.apis.functions.connector.common.vo.TapPartitionResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

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
            tapTable = new TapTable();
            tapTable.setNameFieldMap(new LinkedHashMap<>());
            doCallRealMethod().when(connector).getHashSplitStringSql(tapTable);
        }

        @Test
        void testEmptyField() {
            doCallRealMethod().when(connector).getHashSplitStringSql(tapTable);
            assertThrows(CoreException.class, () -> connector.getHashSplitStringSql(tapTable));
        }

        @Test
        void testNotPrimaryKeys() {
            tapTable.add(new TapField("ID", "INT"));
            tapTable.add(new TapField("TITLE", "VARCHAR(64)"));

            assertThrows(CoreException.class, () -> connector.getHashSplitStringSql(tapTable));
        }

        @Test
        void testTrue() {
            tapTable.add(new TapField("ID", "INT").primaryKeyPos(1));
            tapTable.add(new TapField("TITLE", "VARCHAR(64)"));

            assertNotNull(connector.getHashSplitStringSql(tapTable));
        }
    }

    @Test
    void testSplitTableForMultiDiscoverSchema() {
        PostgresConnector postgresConnector = new PostgresConnector();

        AtomicInteger counter = new AtomicInteger(0);

        List<DataMap> tables = Stream.generate(() -> {
            DataMap dataMap = new DataMap();
            dataMap.put("id", "integer");
            dataMap.put("name", "string");
            dataMap.put("tableType", counter.get() < 5 ? TableType.PARENT_TABLE : TableType.CHILD_TABLE);
            counter.incrementAndGet();
            return dataMap;
        }).limit(10).collect(Collectors.toList());
        postgresConnector.postgresVersion = "1";
        CopyOnWriteArraySet<List<DataMap>> result = postgresConnector.splitTableForMultiDiscoverSchema(tables, 1);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());

        postgresConnector.postgresVersion = "100001";
        result = postgresConnector.splitTableForMultiDiscoverSchema(tables, 1);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(2, result.size());

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            postgresConnector.splitTableForMultiDiscoverSchema(tables, -1);
        });

        tables.clear();
        result = postgresConnector.splitTableForMultiDiscoverSchema(tables, 1);
        Assertions.assertNotNull(result);
        Assertions.assertEquals(0, result.size());

    }

    @Test
    void testDiscoverPartitionInfoByParentName() throws SQLException {
        PostgresConnector postgresConnector = new PostgresConnector();

        PostgresPartitionContext postgresPartitionContext = mock(PostgresPartitionContext.class);
        postgresConnector.postgresPartitionContext = postgresPartitionContext;

        TapConnectorContext connectorContext = mock(TapConnectorContext.class);
        Consumer<Collection<TapPartitionResult>> consumer = (c) -> {};

        postgresConnector.discoverPartitionInfoByParentName(connectorContext, null, consumer);

        verify(postgresPartitionContext, times(1)).discoverPartitionInfoByParentName(any(), any(), any());

        postgresConnector.discoverPartitionInfo(Collections.emptyList());
        verify(postgresPartitionContext, times(1)).discoverPartitionInfo(anyList());
    }
}
