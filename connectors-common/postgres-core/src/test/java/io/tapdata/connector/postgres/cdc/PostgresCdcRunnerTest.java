package io.tapdata.connector.postgres.cdc;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.cdc.offset.PostgresOffset;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

class PostgresCdcRunnerTest {
    private PostgresCdcRunner runner;
    private StreamReadConsumer consumer;

    @BeforeEach
    void setUp() throws Exception {
        PostgresJdbcContext jdbc = mock(PostgresJdbcContext.class);
        PostgresConfig config = new PostgresConfig();
        config.setDdlTriggerEnable(true);
        when(jdbc.getConfig()).thenReturn(config);
        runner = new PostgresCdcRunner(jdbc, mock(TapConnectorContext.class));
        consumer = mock(StreamReadConsumer.class);
        ReflectionTestUtils.setField(runner, "consumer", consumer);
        ReflectionTestUtils.setField(runner, "recordSize", 100);
        ReflectionTestUtils.setField(runner, "filterStartTimeMs", Long.MAX_VALUE);
    }

    @Test
    void shouldPersistLatestHeartbeatOffsetEvenWithoutCommitTimestamp() throws Exception {
        SourceRecord first = heartbeat(101L);
        SourceRecord last = heartbeat(102L);
        runner.consumeRecords(Arrays.asList(first, last), null);
        assertHeartbeatOffset(last.sourceOffset());
    }

    @Test
    void shouldFlushHeartbeatOffsetBeforeDdlAuditBoundary() throws Exception {
        ReflectionTestUtils.setField(runner, "filterStartTimeMs", null);
        Schema sourceSchema = SchemaBuilder.struct().field("schema", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA).build();
        Schema auditSchema = SchemaBuilder.struct().field("op", Schema.STRING_SCHEMA)
                .field("source", sourceSchema).build();
        // Audit DELETE is ignored, but must flush any preceding heartbeat batch.
        Struct audit = new Struct(auditSchema).put("op", "d").put("source",
                new Struct(sourceSchema).put("schema", "public").put("table", PostgresJdbcContext.DDL_AUDIT_TABLE));
        SourceRecord heartbeat = heartbeat(201L);
        SourceRecord boundary = new SourceRecord(Collections.emptyMap(),
                Map.of("lsn", 202L, "ts_usec", 1000000L), "test", auditSchema, audit);
        runner.consumeRecords(Arrays.asList(heartbeat, boundary), null);
        assertHeartbeatOffset(heartbeat.sourceOffset());
    }

    private SourceRecord heartbeat(long lsn) {
        Schema schema = SchemaBuilder.struct().name("io.debezium.connector.common.Heartbeat")
                .field("ts_ms", Schema.INT64_SCHEMA).build();
        return new SourceRecord(Collections.emptyMap(), Map.of("lsn", lsn), "heartbeat", schema,
                new Struct(schema).put("ts_ms", 1234L));
    }

    @SuppressWarnings("unchecked")
    private void assertHeartbeatOffset(Map<String, ?> expected) throws Exception {
        ArgumentCaptor<PostgresOffset> offset = ArgumentCaptor.forClass(PostgresOffset.class);
        verify(consumer).accept(anyList(), offset.capture());
        Map<String, Object> actual = new ObjectMapper().readValue(offset.getValue().getSourceOffset(), Map.class);
        assertEquals(((Number) expected.get("lsn")).longValue(), ((Number) actual.get("lsn")).longValue());
        verifyNoMoreInteractions(consumer);
    }
}
