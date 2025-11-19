package io.tapdata.connector.elasticsearch;

import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class ElasticsearchConnectorTest {
    private ElasticsearchConnector elasticsearchConnector;
    @BeforeEach
    void beforeEach(){
        elasticsearchConnector = mock(ElasticsearchConnector.class);
    }
    @Nested
    class CreateTableTest{
        private TapConnectorContext tapConnectorContext;
        private TapCreateTableEvent tapCreateTableEvent;
        private ElasticsearchHttpContext elasticsearchHttpContext;
        private ElasticsearchExceptionCollector exceptionCollector;
        private ElasticsearchConfig elasticsearchConfig;
        @BeforeEach
        void beforeEach(){
            tapConnectorContext = mock(TapConnectorContext.class);
            tapCreateTableEvent = mock(TapCreateTableEvent.class);
            elasticsearchHttpContext = mock(ElasticsearchHttpContext.class);
            ReflectionTestUtils.setField(elasticsearchConnector,"elasticsearchHttpContext",elasticsearchHttpContext);
            exceptionCollector = new ElasticsearchExceptionCollector();
            ReflectionTestUtils.setField(elasticsearchConnector,"exceptionCollector",exceptionCollector);
            elasticsearchConfig = mock(ElasticsearchConfig.class);
            ReflectionTestUtils.setField(elasticsearchConnector,"elasticsearchConfig",elasticsearchConfig);
        }
        @Test
        void testWithRuntimeEx() throws Throwable {
            TapTable tapTable = mock(TapTable.class);
            when(tapCreateTableEvent.getTable()).thenReturn(tapTable);
            when(tapTable.getId()).thenReturn("test");
            LinkedHashMap<String,TapField> nameFieldMap = new LinkedHashMap();
            nameFieldMap.put("field1",mock(TapField.class));
            when(tapTable.getNameFieldMap()).thenReturn(nameFieldMap);
            when(elasticsearchHttpContext.existsIndex("test")).thenReturn(false);
            when(elasticsearchHttpContext.getElasticsearchClient()).thenThrow(RuntimeException.class);
            doCallRealMethod().when(elasticsearchConnector).createTable(tapConnectorContext,tapCreateTableEvent);
            assertThrows(RuntimeException.class,() -> elasticsearchConnector.createTable(tapConnectorContext,tapCreateTableEvent));
        }
    }
    @Nested
    class InitConnectionTest{
        private TapConnectionContext connectorContext;
        private ElasticsearchHttpContext elasticsearchHttpContext;
        @BeforeEach
        void beforeEach(){
            connectorContext = mock(TapConnectorContext.class);
            elasticsearchHttpContext = mock(ElasticsearchHttpContext.class);
            ReflectionTestUtils.setField(elasticsearchConnector,"elasticsearchHttpContext",elasticsearchHttpContext);
        }
        @Test
        void testInitConnection(){
            DataMap connectionConfig = new DataMap();
            connectionConfig.put("host","127.0.0.1");
            connectionConfig.put("port",9200);
            when(connectorContext.getConnectionConfig()).thenReturn(connectionConfig);
            doCallRealMethod().when(elasticsearchConnector).initConnection(connectorContext);
            assertThrows(RuntimeException.class,() -> elasticsearchConnector.initConnection(connectorContext));
        }
    }
}
