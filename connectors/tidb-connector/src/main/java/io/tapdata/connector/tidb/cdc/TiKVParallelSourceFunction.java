package io.tapdata.connector.tidb.cdc;

import io.tapdata.connector.tidb.cdc.util.TableKeyRangeUtils;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.CDCClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Coprocessor;
import org.tikv.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.Serializable;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.*;

public class TiKVParallelSourceFunction <T> extends RichParallelSourceFunction<T>
        implements CheckpointListener, CheckpointedFunction, ResultTypeQueryable<T>, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(TiKVParallelSourceFunction.class);
    private static final long STREAMING_VERSION_START_EPOCH = 0L;

    private final TiKVSnapshotEventDeserializationSchema<T> snapshotEventDeserializationSchema;
    private final TiKVChangeEventDeserializationSchema<T> changeEventDeserializationSchema;
    private final TiConfiguration tiConf;
    private final String database;
    private final String tableName;

    /** Task local variables. */
    private transient TiSession session = null;

    private transient Coprocessor.KeyRange keyRange = null;
    private transient CDCClient cdcClient = null;
    private transient SourceContext<T> sourceContext = null;
    private transient volatile long resolvedTs = -1L;
    private transient TreeMap<RowKeyWithTs, Cdcpb.Event.Row> prewrites = null;
    private transient TreeMap<RowKeyWithTs, Cdcpb.Event.Row> commits = null;
    private transient BlockingQueue<Cdcpb.Event.Row> committedEvents = null;
    private transient OutputCollector<T> outputCollector;

    private transient boolean running = true;
    private transient ExecutorService executorService;

    /** offset state. */
    private transient ListState<Long> offsetState;

    private static final long CLOSE_TIMEOUT = 30L;

    private  volatile long startTs;

    public TiKVParallelSourceFunction(
            TiKVSnapshotEventDeserializationSchema<T> snapshotEventDeserializationSchema,
            TiKVChangeEventDeserializationSchema<T> changeEventDeserializationSchema,
            TiConfiguration tiConf,
            String database,
            String tableName,
            long startTs) {
        this.snapshotEventDeserializationSchema = snapshotEventDeserializationSchema;
        this.changeEventDeserializationSchema = changeEventDeserializationSchema;
        this.tiConf = tiConf;
        this.database = database;
        this.tableName = tableName;
        this.startTs = startTs;
    }
    @Override
    public void notifyCheckpointComplete(long checkpointId){

    }

    @Override
    public TypeInformation<T> getProducedType() {
        return snapshotEventDeserializationSchema.getProducedType();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        LOG.info("snapshotState checkpoint: {} at resolvedTs: {}", functionSnapshotContext.getCheckpointId(), resolvedTs);
        flushRows(resolvedTs);
        offsetState.clear();
        offsetState.add(resolvedTs);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        LOG.info("initialize checkpoint");
        offsetState = functionInitializationContext.getOperatorStateStore().getListState(
                                new ListStateDescriptor<>(
                                        "resolvedTsState", LongSerializer.INSTANCE));
        if (functionInitializationContext.isRestored()) {
            for (final Long offset : offsetState.get()) {
                resolvedTs = offset;
                LOG.info("Restore State from resolvedTs: {}", resolvedTs);
                break;
            }
        } else {
            resolvedTs = 0;
            LOG.info("Initialize State from resolvedTs: {}", resolvedTs);
        }
    }

    @Override
    public void open(final Configuration config) throws Exception {
        super.open(config);
        session = new TiSession(tiConf);
        TiTableInfo tableInfo = session.getCatalog().getTable(database, tableName);
        if (tableInfo == null) {
            throw new RuntimeException(
                    String.format("Table %s.%s does not exist.", database, tableName));
        }
        long tableId = tableInfo.getId();
        keyRange =
                TableKeyRangeUtils.getTableKeyRange(
                        tableId,
                        getRuntimeContext().getNumberOfParallelSubtasks(),
                        getRuntimeContext().getIndexOfThisSubtask());
        cdcClient = new CDCClient(session, keyRange);
        prewrites = new TreeMap<>();
        commits = new TreeMap<>();
        committedEvents = new LinkedBlockingQueue<>();
        outputCollector = new OutputCollector<>();
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setNameFormat(
                                "tidb-source-function-"
                                        + getRuntimeContext().getIndexOfThisSubtask())
                        .build();
        executorService = Executors.newSingleThreadExecutor(threadFactory);
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        sourceContext = ctx;
        outputCollector.context = sourceContext;

        if (startTs > 0) {
            resolvedTs = startTs << 18;
        }
        LOG.info("start read change events");
        cdcClient.start(resolvedTs);
        running = true;
        readChangeEvents();
    }

    protected void readChangeEvents() throws Exception {
        LOG.info("read change event from resolvedTs:{}", resolvedTs);
        // child thread to sink committed rows.
        executorService.execute(
                () -> {
                    while (running) {
                        try {
                            Cdcpb.Event.Row committedRow = committedEvents.take();
                            changeEventDeserializationSchema.deserialize(
                                    committedRow, outputCollector);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
        while (resolvedTs >= STREAMING_VERSION_START_EPOCH) {
            for (int i = 0; i < 1000; i++) {
                final Cdcpb.Event.Row row = cdcClient.get();
                if (row == null) {
                    break;
                }
                handleRow(row);
            }
            resolvedTs = cdcClient.getMaxResolvedTs();
            if (commits.size() > 0) {
                flushRows(resolvedTs);
            }
        }
    }

    private void handleRow(final Cdcpb.Event.Row row) {
        if (!TableKeyRangeUtils.isRecordKey(row.getKey().toByteArray())) {
            // Don't handle index key for now
            return;
        }
        LOG.debug("binlog record, type: {}, data: {}", row.getType(), row);
        switch (row.getType()) {
            case COMMITTED:
                prewrites.put(RowKeyWithTs.ofStart(row), row);
                commits.put(RowKeyWithTs.ofCommit(row), row);
                break;
            case COMMIT:
                commits.put(RowKeyWithTs.ofCommit(row), row);
                break;
            case PREWRITE:
                prewrites.put(RowKeyWithTs.ofStart(row), row);
                break;
            case ROLLBACK:
                prewrites.remove(RowKeyWithTs.ofStart(row));
                break;
            default:
                LOG.warn("Unsupported row type:" + row.getType());
        }
    }

    protected void flushRows(final long timestamp) throws Exception {
        Preconditions.checkState(sourceContext != null, "sourceContext shouldn't be null");
        synchronized (sourceContext) {
            while (!commits.isEmpty() && commits.firstKey().timestamp <= timestamp) {
                final Cdcpb.Event.Row commitRow = commits.pollFirstEntry().getValue();
                final Cdcpb.Event.Row prewriteRow =
                        prewrites.remove(RowKeyWithTs.ofStart(commitRow));
                // if pull cdc event block when region split, cdc event will lose.
                committedEvents.offer(prewriteRow);
            }
        }
    }




    @Override
    public void cancel() {
        try {
            running = false;
            if (cdcClient != null) {
                cdcClient.close();
            }
            if (executorService != null) {
                executorService.shutdown();
                if (!executorService.awaitTermination(CLOSE_TIMEOUT, TimeUnit.SECONDS)) {
                    LOG.warn("Failed to close the tidb source function in {} seconds.",
                            CLOSE_TIMEOUT);
                }
            }
        } catch (final Exception e) {
            LOG.error("Unable to close cdcClient", e);
        }
    }


    private static class RowKeyWithTs implements Comparable<RowKeyWithTs> {
        private final long timestamp;
        private final RowKey rowKey;

        private RowKeyWithTs(final long timestamp, final RowKey rowKey) {
            this.timestamp = timestamp;
            this.rowKey = rowKey;
        }

        private RowKeyWithTs(final long timestamp, final byte[] key) {
            this(timestamp, RowKey.decode(key));
        }

        @Override
        public int compareTo(final RowKeyWithTs that) {
            int res = Long.compare(this.timestamp, that.timestamp);
            if (res == 0) {
                res = Long.compare(this.rowKey.getTableId(), that.rowKey.getTableId());
            }
            if (res == 0) {
                res = Long.compare(this.rowKey.getHandle(), that.rowKey.getHandle());
            }
            return res;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.timestamp, this.rowKey.getTableId(), this.rowKey.getHandle());
        }

        @Override
        public boolean equals(final Object thatObj) {
            if (thatObj instanceof TiKVParallelSourceFunction.RowKeyWithTs) {
                final RowKeyWithTs that = (RowKeyWithTs) thatObj;
                return this.timestamp == that.timestamp && this.rowKey.equals(that.rowKey);
            }
            return false;
        }

        static RowKeyWithTs ofStart(final Cdcpb.Event.Row row) {
            return new RowKeyWithTs(row.getStartTs(), row.getKey().toByteArray());
        }

        static RowKeyWithTs ofCommit(final Cdcpb.Event.Row row) {
            return new RowKeyWithTs(row.getCommitTs(), row.getKey().toByteArray());
        }
    }


    private static class OutputCollector<T> implements Collector<T> {

        private SourceContext<T> context;

        @Override
        public void collect(T rowRecord) {
            context.collect(rowRecord);
        }

        @Override
        public void close() {
            // do nothing
        }
    }

}
