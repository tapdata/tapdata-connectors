package io.tapdata.connector.gauss;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.ResultSetConsumer;
import io.tapdata.connector.gauss.core.GaussDBConfig;
import io.tapdata.connector.gauss.core.GaussDBJdbcContext;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.entity.TestItem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GaussDBTestTest {
    GaussDBTest test;
    JdbcContext jdbcContext;
    GaussDBConfig commonDbConfig;
    Consumer<TestItem> consumer;
    ResultSet rs;
    @BeforeEach
    void init() {
        rs = mock(ResultSet.class);
        test = mock(GaussDBTest.class);
        jdbcContext = mock(GaussDBJdbcContext.class);
        commonDbConfig = mock(GaussDBConfig.class);
        consumer = t -> {};

        when(commonDbConfig.getUser()).thenReturn("user");
        when(commonDbConfig.getDatabase()).thenReturn("database");
        when(commonDbConfig.getSchema()).thenReturn("schema");
        //doNothing().when(consumer).accept(any(TestItem.class));

        when(test.jdbcContext()).thenReturn(jdbcContext);
        when(test.commonDbConfig()).thenReturn(commonDbConfig);
        when(test.consumer()).thenReturn(consumer);
        when(test.init()).thenReturn(test);
    }

    @Test
    void testParam() {
        Assertions.assertEquals("SELECT COUNT(*) FROM pg_tables WHERE schemaname='%s'", GaussDBTest.PG_TABLE_NUM);
        Assertions.assertEquals("SELECT count(*) FROM information_schema.table_privileges " +
                "WHERE grantee='%s' AND table_catalog='%s' AND table_schema='%s' AND privilege_type='SELECT'", GaussDBTest.PG_TABLE_SELECT_NUM);
    }

    @Nested
    class TestFunctionMapTest {

    }

    @Nested
    class CommonDbConfigTest {

    }

    @Nested
    class JdbcContextTest {

    }

    @Nested
    class ConsumerTest {

    }

    @Nested
    class InitContextTest {

    }

    @Nested
    class SupportVersionsTest {
        @BeforeEach
        void init() {
            when(test.supportVersions()).thenCallRealMethod();
        }

        @Test
        void testSupportVersions() {
            List<String> list = test.supportVersions();
            Assertions.assertNotNull(list);
            Assertions.assertEquals(5, list.size());
            String l0 = list.get(0);
            Assertions.assertEquals("9.2", l0);
            String l1 = list.get(1);
            Assertions.assertEquals("9.4", l1);
            String l2 = list.get(2);
            Assertions.assertEquals("9.5", l2);
            String l3 = list.get(3);
            Assertions.assertEquals("9.6", l3);
            String l4 = list.get(4);
            Assertions.assertEquals("1*", l4);
        }
    }

    @Nested
    class TestReadPrivilegeTest {
        //ResultSetConsumer c;
        String sql;
        @BeforeEach
        void init() {
            when(test.testReadPrivilege()).thenCallRealMethod();
            sql = String.format(GaussDBTest.PG_TABLE_SELECT_NUM, "user", "database", "schema");
            //c = mock(ResultSetConsumer.class);
            //when(test.readConsumer(any(AtomicInteger.class))).thenReturn(c);
        }

        void assertVerify(int tableCountTimes, boolean result) throws SQLException {
            Boolean testResult = test.testReadPrivilege();
            verify(test, times(1)).jdbcContext();
            verify(jdbcContext, times(1)).queryWithNext(anyString(), any(ResultSetConsumer.class));
            verify(test, times(1)).commonDbConfig();
            String database = verify(commonDbConfig, times(1)).getDatabase();
            String schema = verify(commonDbConfig, times(1)).getSchema();
            String user = verify(commonDbConfig, times(1)).getUser();
            verify(test, times(tableCountTimes)).tableCount();
            verify(test, times(1)).consumer();
            //verify(consumer, times(1)).accept(any(TestItem.class));
            Assertions.assertEquals(result, testResult);
        }

        @Test
        public void testQueryWithNextThrowException() throws SQLException {
            when(test.tableCount()).thenReturn(1);
            doAnswer(w -> {
                throw new SQLException("sql exception");
            }).when(jdbcContext).queryWithNext(anyString(), any(ResultSetConsumer.class));
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(0, false);
            });
        }
        @Test
        public void testTableCountThrowException() throws SQLException {
            doAnswer(w -> {
                throw new SQLException("sql exception");
            }).when(test).tableCount();
            doNothing().when(jdbcContext).queryWithNext(anyString(), any(ResultSetConsumer.class));
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(1, false);
            });
        }
        @Test
        public void testTableSelectPrivilegesLessThanTableCount() throws SQLException {
            when(test.tableCount()).thenReturn(1);
            doNothing().when(jdbcContext).queryWithNext(anyString(), any(ResultSetConsumer.class));
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(1, true);
            });
        }
        @Test
        public void testTableSelectPrivilegesMoreThanTableCount() throws SQLException {
            when(test.tableCount()).thenReturn(0);
            doNothing().when(jdbcContext).queryWithNext(anyString(), any(ResultSetConsumer.class));
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(1, true);
            });
        }
        @Test
        public void testTableSelectPrivilegesEqualTableCount() throws SQLException {
            when(test.tableCount()).thenReturn(-1);
            doNothing().when(jdbcContext).queryWithNext(anyString(), any(ResultSetConsumer.class));
            Assertions.assertDoesNotThrow(() -> {
                assertVerify(1, true);
            });
        }

    }

    @Nested
    class TableCountTest{
        @BeforeEach
        public void init() throws SQLException {
            when(test.tableCount()).thenCallRealMethod();
        }

        int assertVerify() throws SQLException {
            int count = test.tableCount();
            verify(test, times(1)).jdbcContext();
            verify(jdbcContext, times(1)).queryWithNext(anyString(), any(ResultSetConsumer.class));
            return count;
        }

        @Test
        public void testQueryWithNextThrowException() throws SQLException {
            doAnswer(w -> {
                throw new SQLException("sql exception");
            }).when(jdbcContext).queryWithNext(anyString(), any(ResultSetConsumer.class));
            Assertions.assertThrows(SQLException.class, this::assertVerify);
        }

        @Test
        public void testQueryWithNextNotThrowException() throws SQLException {
            when(rs.getInt(1)).thenReturn(1);
            doAnswer(a -> {
                ResultSetConsumer argument = a.getArgument(1, ResultSetConsumer.class);
                argument.accept(rs);
                return null;
            }).when(jdbcContext).queryWithNext(anyString(), any(ResultSetConsumer.class));
            Assertions.assertDoesNotThrow(this::assertVerify);
        }
    }

    @Nested
    class TestWritePrivilegeTest {
        List<DataMap> tableList;
        @BeforeEach
        public void init() {
            tableList = mock(List.class);
            when(tableList.isEmpty()).thenReturn(false);

            when(test.testWritePrivilege()).thenCallRealMethod();
        }

        void assertVerify(int isEmptyTimes, int batchExecuteTimes) throws SQLException {
            Boolean result = test.testWritePrivilege();
            verify(test, times(1)).jdbcContext();
            verify(test, times(1)).commonDbConfig();
            String schema = verify(commonDbConfig, times(1)).getSchema();
            verify(test, times(1)).consumer();
            //verify(consumer, times(1)).accept(any(TestItem.class));
            verify(jdbcContext, times(1)).queryAllTables(anyList());
            boolean empty = verify(tableList, times(isEmptyTimes)).isEmpty();
            verify(jdbcContext, times(batchExecuteTimes)).batchExecute(anyList());
            Assertions.assertEquals(true, result);
        }

        @Test
        public void testQueryAllTablesWithTables() throws SQLException {
            when(jdbcContext.queryAllTables(anyList())).thenReturn(tableList);
            doNothing().when(jdbcContext).batchExecute(anyList());
            assertVerify(1,1);
        }
        @Test
        public void testEmptySchema() throws SQLException {
            when(commonDbConfig.getSchema()).thenReturn("");
            when(jdbcContext.queryAllTables(anyList())).thenReturn(tableList);
            doNothing().when(jdbcContext).batchExecute(anyList());
            assertVerify(1,1);
        }
        @Test
        public void testQueryAllTablesWithoutTable() throws SQLException {
            when(tableList.isEmpty()).thenReturn(true);
            when(jdbcContext.queryAllTables(anyList())).thenReturn(tableList);
            doNothing().when(jdbcContext).batchExecute(anyList());
            assertVerify(1,1);
        }
        @Test
        public void testQueryAllTablesThrowException() throws SQLException {
            doAnswer(w -> {
                throw new SQLException("SQL Exception");
            }).when(jdbcContext).queryAllTables(anyList());
            doNothing().when(jdbcContext).batchExecute(anyList());
            assertVerify(0,0);
        }
        @Test
        public void testBatchExecuteThrowException() throws SQLException {
            when(jdbcContext.queryAllTables(anyList())).thenReturn(tableList);
            doAnswer(w -> {
                throw new SQLException("SQL Exception");
            }).when(jdbcContext).batchExecute(anyList());
            assertVerify(1,1);
        }
    }
}
