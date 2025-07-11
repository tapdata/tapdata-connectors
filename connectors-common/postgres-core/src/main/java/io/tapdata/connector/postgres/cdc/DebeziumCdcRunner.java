package io.tapdata.connector.postgres.cdc;

import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import io.tapdata.common.cdc.CdcRunner;
import io.tapdata.kit.EmptyKit;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Abstract runner for change data capture
 *
 * @author Jarad
 * @date 2022/5/13
 */
public abstract class DebeziumCdcRunner implements CdcRunner {

    protected EmbeddedEngine engine;
    protected String runnerName;

    protected DebeziumCdcRunner() {

    }

    public String getRunnerName() {
        return runnerName;
    }

    /**
     * records caught by cdc can only be consumed in this method
     */
    public void consumeRecords(List<SourceRecord> sourceRecords, DebeziumEngine.RecordCommitter<SourceRecord> committer) throws InterruptedException {

    }

    /**
     * start cdc sync
     */
    @Override
    public void startCdcRunner() {
        System.setProperty("debezium.embedded.shutdown.pause.before.interrupt.ms", "3000");
        if (EmptyKit.isNotNull(engine)) {
            engine.run();
        }
    }

    public void flushOffset(Map<String, ?> offset) {
        if (EmptyKit.isNotNull(engine)) {
            engine.flushOffset(offset);
        }
    }

    public void stopCdcRunner() {
        if (null != engine && engine.isRunning()) {
            engine.stop();
        }
    }

    @Override
    public boolean isRunning() {
        return null != engine && engine.isRunning();
    }

    /**
     * close cdc sync
     */
    @Override
    public void closeCdcRunner() throws IOException {
        engine.close();
    }

    @Override
    public void run() {
        startCdcRunner();
    }

}
