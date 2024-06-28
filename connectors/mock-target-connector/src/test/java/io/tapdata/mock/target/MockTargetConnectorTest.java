package io.tapdata.mock.target;

import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class MockTargetConnectorTest {
    private MockTargetConnector targetConnector;
    private TapConnectionContext connectionContext;
    @BeforeEach
    void beforeEach(){
        targetConnector = mock(MockTargetConnector.class);
        connectionContext = mock(TapConnectionContext.class);
    }
    @Nested
    class OnStartTest{
        @Test
        void testOnStartNormal() throws Throwable {
            when(connectionContext.getConnectionConfig()).thenReturn(mock(DataMap.class));
            when(connectionContext.getLog()).thenReturn(mock(Log.class));
            doCallRealMethod().when(targetConnector).onStart(connectionContext);
            targetConnector.onStart(connectionContext);
            verify(connectionContext).getLog();
        }
    }
    @Nested
    class DiscoverSchemaTest{
        private List<String> tables;
        private int tableSize;
        private Consumer<List<TapTable>> consumer;
        @Test
        void testDiscoverSchemaNormal() throws Throwable {
            tables = new ArrayList<>();
            tableSize = 1;
            consumer = mock(Consumer.class);
            ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
            doCallRealMethod().when(targetConnector).discoverSchema(connectionContext, tables, tableSize, consumer);
            targetConnector.discoverSchema(connectionContext, tables, tableSize, consumer);
            verify(consumer).accept(captor.capture());
            List actual = captor.getValue();
            TapTable tapTable = (TapTable) actual.get(0);
            assertEquals("mock_target_test",tapTable.getId());
            assertEquals("mock_target_test",tapTable.getName());
        }
    }
    @Nested
    class GetTableNamesTest{
        private int batchSize;
        private Consumer<List<String>> listConsumer;
        @Test
        void testGetTableNamesNormal() throws Throwable {
            batchSize = 1;
            listConsumer = mock(Consumer.class);
            ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
            doCallRealMethod().when(targetConnector).getTableNames(connectionContext, batchSize, listConsumer);
            targetConnector.getTableNames(connectionContext, batchSize, listConsumer);
            verify(listConsumer).accept(captor.capture());
            List actual = captor.getValue();
            String tableName = (String) actual.get(0);
            assertEquals("mock_target_test",tableName);
        }
    }
}
