package io.tapdata.common;

import org.junit.jupiter.api.Test;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class InspectQueryRedesignTest {
    @Test void emptyQueryPublishesColumnNamesAndTypesAndClosesResources() throws Throwable {
        Connection connection=mock(Connection.class);Statement statement=mock(Statement.class);ResultSet rows=mock(ResultSet.class);ResultSetMetaData meta=mock(ResultSetMetaData.class);
        when(connection.createStatement()).thenReturn(statement);when(connection.getAutoCommit()).thenReturn(true);when(statement.execute("select v where false")).thenReturn(true);when(statement.getResultSet()).thenReturn(rows);when(statement.getUpdateCount()).thenReturn(-1);when(rows.getMetaData()).thenReturn(meta);when(meta.getColumnCount()).thenReturn(1);when(meta.getColumnLabel(1)).thenReturn("v");when(meta.getColumnTypeName(1)).thenReturn("DECIMAL");
        AtomicReference<List<Map<String,String>>> schema=new AtomicReference<>();AtomicReference<Runnable> cancel=new AtomicReference<>();
        Map<String,Object> params=new HashMap<>();params.put("inspectColumns",(Consumer<List<Map<String,String>>>)schema::set);params.put("inspectRegisterCancel",(Consumer<Runnable>)cancel::set);
        try(InspectQueryScope scope=InspectQueryScope.open(params)) {
            JdbcQueryExecutor.execute("select v where false",()->connection,ignored->fail("No data expected"),()->true,10,600,null,rs->()->Map.of());
        }
        assertEquals(List.of(Map.of("name","v","type","DECIMAL")),schema.get());cancel.get().run();verify(statement).cancel();verify(statement).close();verify(rows).close();verify(connection,atLeastOnce()).close();assertFalse(InspectQueryScope.active());
    }
    @Test void inspectionRejectsUpdateCountsButLegacyCommandsStillWork() throws Throwable {
        Connection connection=mock(Connection.class);Statement statement=mock(Statement.class);when(connection.createStatement()).thenReturn(statement);when(statement.getUpdateCount()).thenReturn(1);when(connection.getAutoCommit()).thenReturn(true);
        Map<String,Object> params=new HashMap<>();params.put("inspectColumns",(Consumer<List<Map<String,String>>>)ignored->{});
        try(InspectQueryScope scope=InspectQueryScope.open(params)) { assertThrows(SQLException.class,()->JdbcQueryExecutor.execute("update t set v=1",()->connection,ignored->{},()->true,10,0,null,rs->()->Map.of())); }
        List<Object> result=new ArrayList<>();JdbcQueryExecutor.execute("update t set v=1",()->connection,result::add,()->true,10,0,null,rs->()->Map.of());assertEquals(List.of(1L),result);
    }

    @Test void inspectionRunsInReadOnlyTransactionAndNeverCommits() throws Throwable {
        Connection connection=mock(Connection.class);Statement statement=mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);when(statement.getUpdateCount()).thenReturn(1);
        Map<String,Object> params=new HashMap<>();params.put("inspectColumns",(Consumer<List<Map<String,String>>>)ignored->{});
        try(InspectQueryScope scope=InspectQueryScope.open(params)) {
            assertThrows(SQLException.class,()->JdbcQueryExecutor.execute("select 1",()->connection,ignored->{},()->true,10,1,null,rs->()->Collections.emptyMap()));
        }
        org.mockito.InOrder order=inOrder(connection,statement);order.verify(connection).setReadOnly(true);order.verify(connection).createStatement();order.verify(statement).execute("select 1");
        verify(connection,never()).commit();verify(connection).close();
    }

}
