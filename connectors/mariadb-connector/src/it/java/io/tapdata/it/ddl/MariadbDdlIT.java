package io.tapdata.it.ddl;

import io.tapdata.connector.mariadb.MariadbConnector;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.it.UnderTest;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class MariadbDdlIT extends MysqlFamilyDdlIT {
    public MariadbDdlIT() {
        super(MariadbConnector::new, "spec_mariadb.json", "mariadb12", "mariadb-connector");
    }

    @Test
    @UnderTest(value = "streamRead", requiresVerifier = true)
    void should_start_new_cdc_task_without_previous_offset() throws Throwable {
        createTableIfNeeded();
        TapTable table = buildTapTable();
        registerTable(table);
        String marker = "fresh-snapshot-" + System.nanoTime();
        CountDownLatch received = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();
        StreamReadConsumer consumer = StreamReadConsumer.create((events, offset) -> {
            if (events.stream().filter(TapInsertRecordEvent.class::isInstance)
                    .map(TapInsertRecordEvent.class::cast)
                    .anyMatch(event -> table.getId().equals(event.getTableId())
                            && event.getAfter().containsValue(marker))) received.countDown();
        });
        Thread reader = new Thread(() -> {
            try {
                // A fresh task must determine its initial binlog position through the shared snapshot source.
                sourceDdlStreamReadFunction().streamRead(nodeContext(), List.of(table.getId()), null, 100, consumer);
            } catch (Throwable failure) {
                error.set(failure);
            }
        }, "mariadb-fresh-snapshot");
        reader.setDaemon(true);
        reader.start();
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(sourceDdlStartupTimeoutSeconds());
            while (consumer.getState() != StreamReadConsumer.STATE_STREAM_READ_STARTED
                    && error.get() == null && System.nanoTime() < deadline) Thread.sleep(50L);
            assertNull(error.get(), () -> "fresh CDC startup failed: " + error.get());
            assertEquals(StreamReadConsumer.STATE_STREAM_READ_STARTED, consumer.getState());
            Map<String, Object> row = generateRows(1).get(0);
            row.put("c_varchar", marker);
            verifier().insert(table.getId(), List.of(row));
            assertTrue(received.await(streamReadTimeoutSeconds(), TimeUnit.SECONDS),
                    () -> "fresh CDC task did not deliver its insert; error=" + error.get());
            assertNull(error.get(), () -> "fresh CDC task failed: " + error.get());
        } finally {
            consumer.streamReadEnded();
        }
    }
}
