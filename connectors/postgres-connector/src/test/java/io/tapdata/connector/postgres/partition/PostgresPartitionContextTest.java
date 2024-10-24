package io.tapdata.connector.postgres.partition;

import io.tapdata.common.ResultSetConsumer;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.logger.TapLog;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.TapPartition;
import io.tapdata.pdk.apis.functions.connector.common.vo.TapPartitionResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/9/5 20:43
 */
public class PostgresPartitionContextTest {

    private static PostgresConfig postgresConfig;
    @BeforeAll
    public static void setups() {
        postgresConfig = new PostgresConfig();
        postgresConfig.setHost("localhost");
        postgresConfig.setPort(5432);
        postgresConfig.setUser("postgres");
        postgresConfig.setPassword("postgres");
        postgresConfig.setDatabase("postgres");
        postgresConfig.setSchema("public");
    }

    @Test
    void testDiscoverPartitionInfoByParentName() {
        TapTable normalTable = new TapTable();
        normalTable.setId("test");

        TapTable partitionTable = new TapTable();
        partitionTable.setId("measurement");

        List<String> partitionNames = Arrays.asList("measurement_y2024m01", "measurement_y2024m02");

        try (PostgresJdbcContext jdbcCtx = mock(PostgresJdbcContext.class)){

            List<TapPartitionResult> result = new ArrayList<>();
            PostgresPartitionContext ctx = new PostgresPartitionContext(new TapLog())
                    .withPostgresVersion("160004")
                    .withPostgresConfig(postgresConfig)
                    .withJdbcContext(jdbcCtx);

            doAnswer(answer -> {
                ResultSetConsumer consumer = answer.getArgument(1);

                ResultSet resultSet = mock(ResultSet.class);
                when(resultSet.next()).thenReturn(true, true, false);
                when(resultSet.getString(TableType.KEY_PARENT_TABLE)).thenReturn(partitionTable.getId(), partitionTable.getId());
                when(resultSet.getString(TableType.KEY_PARTITION_TABLE)).thenReturn(partitionNames.get(0), partitionNames.get(1));

                consumer.accept(resultSet);

                return null;
            }).when(jdbcCtx).query(anyString(), any());

            Assertions.assertDoesNotThrow(() ->
                    ctx.discoverPartitionInfoByParentName(null,
                            Arrays.asList(normalTable, partitionTable),
                            result::addAll), "discoverPartitionInfoByParentName should not throw exception");

            Assertions.assertFalse(result.isEmpty(), "result should not be empty");
            Assertions.assertEquals(1, result.size(), "result size should be equal 1");
            Assertions.assertEquals(partitionNames, result.get(0).getSubPartitionTableNames(), "partition names should be equal");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testDiscoverPartitionInfo() {
        List<TapTable> tables = new ArrayList<>();

        try (PostgresJdbcContext jdbcCtx = mock(PostgresJdbcContext.class)){
            PostgresPartitionContext ctx = new PostgresPartitionContext(new TapLog())
                    .withPostgresVersion("160004")
                    .withPostgresConfig(postgresConfig)
                    .withJdbcContext(jdbcCtx);

            List<TapTable> result = ctx.discoverPartitionInfo(tables);
            Assertions.assertTrue(result.isEmpty(), "result should be empty");

            TapTable partitionTable = new TapTable();
            partitionTable.setId("measurement");
            partitionTable
                    .add(new TapField().name("id").dataType("int").isPrimaryKey(Boolean.TRUE))
                    .add(new TapField().name("city_id").dataType("int"))
                    .add(new TapField().name("logdate").dataType("date").isPartitionKey(Boolean.TRUE))
                    .add(new TapField().name("peaktemp").dataType("int"))
                    .add(new TapField().name("unitsales").dataType("int"));

            tables.add(partitionTable);

            List<String> partitionNames = Arrays.asList("measurement_y2024m01", "measurement_y2024m02");
            partitionNames.forEach(name -> {
                TapTable subPartitionTable = new TapTable();
                subPartitionTable.setId(name);
                subPartitionTable
                        .add(new TapField().name("id").dataType("int").isPrimaryKey(Boolean.TRUE))
                        .add(new TapField().name("city_id").dataType("int"))
                        .add(new TapField().name("logdate").dataType("date").isPartitionKey(Boolean.TRUE))
                        .add(new TapField().name("peaktemp").dataType("int"))
                        .add(new TapField().name("unitsales").dataType("int"));
                tables.add(subPartitionTable);
            });

            Assertions.assertDoesNotThrow(() -> {
                ctx.withPostgresVersion("90000");
                List<TapTable> r = ctx.discoverPartitionInfo(tables);
                Assertions.assertNotNull(r, "result should not be null");
            });

            ctx.withPostgresVersion("160004");

            doAnswer(answer -> {
                String sql = answer.getArgument(0);
                System.out.println(sql);
                ResultSetConsumer consumer = answer.getArgument(1);

                ResultSet resultSet = mock(ResultSet.class);
                when(resultSet.next()).thenReturn(true, true, true, false);
                when(resultSet.getString(TableType.KEY_PARENT_TABLE)).thenReturn("", partitionTable.getId(), partitionTable.getId());
                when(resultSet.getString(TableType.KEY_PARTITION_TABLE)).thenReturn(partitionTable.getId(), "", "");
                when(resultSet.getString(TableType.KEY_TABLE_TYPE)).thenReturn("Partitioned Table", "Child Table", "Child Table", "Child Table");
                when(resultSet.getString(TableType.KEY_TABLE_NAME)).thenReturn("measurement", "measurement_y2024m01", "measurement_y2024m02");
                when(resultSet.getString(TableType.KEY_CHECK_OR_PARTITION_RULE)).thenReturn("RANGE (logdate)", "RANGE (logdate)", "RANGE (logdate)");
                when(resultSet.getString(TableType.KEY_PARTITION_TYPE)).thenReturn("Range", "Range", "Range");
                when(resultSet.getString(TableType.KEY_PARTITION_BOUND)).thenReturn("", "FOR VALUES FROM ('2024-01-01') TO ('2024-02-01')", "FOR VALUES FROM ('2024-02-01') TO ('2024-03-01')");

                consumer.accept(resultSet);
                return null;
            }).when(jdbcCtx).query(anyString(), any());


            ArrayList<Object> finalResult = new ArrayList<>();
            Assertions.assertDoesNotThrow(() -> {
                List<TapTable> r = ctx.discoverPartitionInfo(tables);
                finalResult.clear();
                finalResult.addAll(r);
            }, "discoverPartitionInfoByParentName should not throw exception");

            Assertions.assertFalse(result.isEmpty(), "result should not be empty");

            Assertions.assertEquals(3, result.size(), "result size should be 1");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testUpdatePk() {
        try (PostgresJdbcContext jdbcCtx = mock(PostgresJdbcContext.class)) {
            PostgresPartitionContext ctx = new PostgresPartitionContext(new TapLog())
                    .withPostgresVersion("160004")
                    .withPostgresConfig(postgresConfig)
                    .withJdbcContext(jdbcCtx);

            TapTable table = new TapTable();
            table.setId("test");
            table.setName("test");
            table.setPartitionMasterTableId("test");
            table.setPartitionInfo(new TapPartition());
            table.setNameFieldMap(new LinkedHashMap<>());
            table.getNameFieldMap().put("id", new TapField("id", "int"));
            table.getNameFieldMap().put("name", new TapField("name", "string"));

            TapTable subTable = new TapTable();
            subTable.setId("test_1");
            subTable.setName("test_1");
            subTable.setPartitionMasterTableId("test");
            subTable.setPartitionInfo(new TapPartition());
            subTable.setNameFieldMap(new LinkedHashMap<>());
            TapField idField = new TapField("id", "int");
            subTable.getNameFieldMap().put("id", idField);
            subTable.getNameFieldMap().put("name", new TapField("name", "string"));

            ctx.updatePk(table, subTable);
            Assertions.assertNotNull(table.primaryKeys());
            Assertions.assertEquals(0, table.primaryKeys().size());

            subTable = new TapTable();
            subTable.setId("test_1");
            subTable.setName("test_1");
            subTable.setPartitionMasterTableId("test");
            subTable.setPartitionInfo(new TapPartition());
            subTable.setNameFieldMap(new LinkedHashMap<>());
            idField = new TapField("id", "int");
            subTable.getNameFieldMap().put("id", idField);
            subTable.getNameFieldMap().put("name", new TapField("name", "string"));
            idField.setPrimaryKey(Boolean.TRUE);
            idField.setPrimaryKeyPos(1);
            ctx.updatePk(table, subTable);
            Assertions.assertNotNull(table.getNameFieldMap().get("id").getPrimaryKey());
            Assertions.assertTrue(table.getNameFieldMap().get("id").getPrimaryKey());
        }
    }

}
