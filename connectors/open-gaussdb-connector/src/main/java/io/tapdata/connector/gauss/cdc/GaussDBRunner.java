package io.tapdata.connector.gauss.cdc;

import com.huawei.opengauss.jdbc.PGProperty;
import com.huawei.opengauss.jdbc.jdbc.PgConnection;
import com.huawei.opengauss.jdbc.replication.PGReplicationStream;
import com.huawei.opengauss.jdbc.replication.fluent.logical.ChainedLogicalStreamBuilder;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import io.tapdata.connector.gauss.cdc.logic.event.EventFactory;
import io.tapdata.connector.gauss.cdc.logic.event.LogicReplicationEventFactoryImpl;
import io.tapdata.connector.gauss.core.GaussDBConfig;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.cdc.DebeziumCdcRunner;
import io.tapdata.connector.postgres.cdc.PostgresCdcRunner;
import io.tapdata.connector.postgres.cdc.config.PostgresDebeziumConfig;
import io.tapdata.connector.postgres.cdc.offset.PostgresOffset;
import io.tapdata.connector.postgres.cdc.offset.PostgresOffsetStorage;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.NumberKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.codehaus.plexus.util.StringUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class GaussDBRunner extends DebeziumCdcRunner {
    private final GaussDBConfig gaussDBConfig;
    private PostgresOffset postgresOffset;
    private int recordSize;
    private StreamReadConsumer consumer;
    private final AtomicReference<Throwable> throwableAtomicReference = new AtomicReference<>();
    //protected final TimeZone timeZone;
    private final String jdbcURL;
    final Log log;
    PGReplicationStream stream;
    PgConnection conn;
    String whiteTableList;
    EventFactory<ByteBuffer> eventFactory;

    public GaussDBRunner(GaussDBConfig config, Log log) throws SQLException {
        this.log = log;
        this.gaussDBConfig = config;
        jdbcURL = String.format("jdbc:opengauss://%s:%d/%s",
                gaussDBConfig.getHost(),
                gaussDBConfig.getHaPort(),
                gaussDBConfig.getDatabase());
    }

    public GaussDBRunner useSlot(String slotName) {
        this.runnerName = slotName;
        return this;
    }

    public GaussDBRunner watch(List<String> observedTableList) {
        if (null != observedTableList && !observedTableList.isEmpty()) {
            whiteTableList = String.join(",", observedTableList);
        }
        return this;
    }

    public GaussDBRunner offset(Object offsetState) {
        if (EmptyKit.isNull(offsetState)) {
            postgresOffset = new PostgresOffset();
        } else {
            this.postgresOffset = (PostgresOffset) offsetState;
        }
        PostgresOffsetStorage.postgresOffsetMap.put(runnerName, postgresOffset);
        return this;
    }

    public AtomicReference<Throwable> getThrowable() {
        return throwableAtomicReference;
    }

    public void registerConsumer(StreamReadConsumer consumer, int recordSize) throws SQLException {
        this.recordSize = recordSize;
        this.consumer = consumer;
        Properties properties = new Properties();
        PGProperty.USER.set(properties, gaussDBConfig.getUser());
        PGProperty.PASSWORD.set(properties, gaussDBConfig.getPassword());
        // 对于逻辑复制，以下三个属性是必须配置项
        PGProperty.REPLICATION.set(properties, "database");
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "9.4");
        PGProperty.PREFER_QUERY_MODE.set(properties, "simple");
        PGProperty.CURRENT_SCHEMA.set(properties, gaussDBConfig.getSchema());
        PGProperty.SSL.set(properties, "false");

        // 自定义
        PGProperty.LOGGER.set(properties, "Slf4JLogger");
        PGProperty.SSL.set(properties, true);
        PGProperty.SSL_FACTORY.set(properties, "com.huawei.opengauss.jdbc.ssl.NonValidatingFactory");
        conn = (PgConnection) DriverManager.getConnection(jdbcURL, properties);
        ChainedLogicalStreamBuilder streamBuilder = conn.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(runnerName)
                .withSlotOption("include-xids", false)
                .withSlotOption("skip-empty-xacts", true)
                //.withStartPosition(null)
                .withSlotOption("parallel-decode-num", 10);//解码线程并行度
        if (null != whiteTableList) {
            streamBuilder.withSlotOption("white-table-list", whiteTableList); //白名单列表
        }

        eventFactory = LogicReplicationEventFactoryImpl.instance(consumer);

        //build PGReplicationStream
        stream = streamBuilder.withSlotOption("standby-connection", true) //强制备机解码
                .withSlotOption("decode-style", "t") //解码格式
                .withSlotOption("sending-batch", 1) //批量发送解码结果
                .withSlotOption("max-txn-in-memory", 100) //单个解码事务落盘内存阈值为100MB
                .withSlotOption("max-reorderbuffer-in-memory", 50) //正在处理的解码事务落盘内存阈值为
                .withSlotOption("exclude-users", "userA") //不返回用户userA执行事务的逻辑日志
                .withSlotOption("include-user", true) //事务BEGIN逻辑日志携带用户名字
                .withSlotOption("enable-heartbeat", true) // 开启心跳日志
                .start();

    }

    /**
     * start cdc sync
     */
    @Override
    public void startCdcRunner() {
        if (null == consumer) {
            throw new CoreException("CDC consumer can not be empty");
        }
        consumer.streamReadStarted();
        try {
            while (!Thread.interrupted()) {
                ByteBuffer byteBuffer = stream.readPending();

                if (byteBuffer == null) {
                    TimeUnit.MILLISECONDS.sleep(5L);
                    continue;
                }
                eventFactory.emit(byteBuffer);
                int offset = byteBuffer.arrayOffset();
                byte[] source = byteBuffer.array();
                int length = source.length - offset;
                String s = new String(source, offset, length);
                if (!s.contains("HeartBeat: ")) {
                    System.out.println("-------> " + s);
                }

                //如果需要flush lsn，根据业务实际情况调用以下接口
                //LogSequenceNumber lastRecv = stream.getLastReceiveLSN();
                //stream.setFlushedLSN(lastRecv);
                //stream.forceUpdateStatus();
            }
        } catch (Exception e) {
            //@todo
            e.printStackTrace();
        } finally {
            consumer.streamReadEnded();
            closeCdcRunner();
        }
    }

    public void stopCdcRunner() {
        super.startCdcRunner();
    }

    @Override
    public boolean isRunning() {
        return null != engine && engine.isRunning();
    }

    /**
     * close cdc sync
     */
    @Override
    public void closeCdcRunner() {
        try {
            super.closeCdcRunner();
        } catch (Exception e) {

        }
        try {
            conn.close();
        } catch (Exception e) {

        }
        try {
            stream.close();
        } catch (Exception e) {

        }
        try {
            engine.close();
        } catch (Exception e) {

        }
    }

    @Override
    public void run() {
        startCdcRunner();
    }

    @Override
    public void consumeRecords(List<SourceRecord> sourceRecords, DebeziumEngine.RecordCommitter<SourceRecord> committer) {
        super.consumeRecords(sourceRecords, committer);
        List<TapEvent> eventList = TapSimplify.list();
        Map<String, ?> offset = null;
        for (SourceRecord sr : sourceRecords) {
            offset = sr.sourceOffset();
            // PG use micros to indicate the time but in pdk api we use millis
            Long referenceTime = (Long) offset.get("ts_usec") / 1000;
            Struct struct = ((Struct) sr.value());
            if (struct == null) {
                continue;
            }
            if ("io.debezium.connector.common.Heartbeat".equals(sr.valueSchema().name())) {
                eventList.add(new HeartbeatEvent().init().referenceTime(((Struct)sr.value()).getInt64("ts_ms")));
                continue;
            }
            String op = struct.getString("op");
            String lsn = String.valueOf(offset.get("lsn"));
            String table = struct.getStruct("source").getString("table");
            Struct after = struct.getStruct("after");
            Struct before = struct.getStruct("before");
            TapRecordEvent event = null;
            switch (op) { //snapshot.mode = 'never'
                case "c": //after running --insert
                case "r": //after slot but before running --read
                    event = new TapInsertRecordEvent().init().table(table).after(getMapFromStruct(after));
                    break;
                case "d": //after running --delete
                    event = new TapDeleteRecordEvent().init().table(table).before(getMapFromStruct(before));
                    break;
                case "u": //after running --update
                    event = new TapUpdateRecordEvent().init().table(table).after(getMapFromStruct(after)).before(getMapFromStruct(before));
                    break;
                default:
                    break;
            }
            if (EmptyKit.isNotNull(event)) {
                event.setReferenceTime(referenceTime);
                event.setExactlyOnceId(lsn);
            }
            eventList.add(event);
            if (eventList.size() >= recordSize) {
                postgresOffset.setSourceOffset(TapSimplify.toJson(offset));
                consumer.accept(eventList, postgresOffset);
                PostgresOffsetStorage.postgresOffsetMap.put(runnerName, postgresOffset);
                eventList = TapSimplify.list();
            }
        }
        if (EmptyKit.isNotEmpty(eventList)) {
            postgresOffset.setSourceOffset(TapSimplify.toJson(offset));
            consumer.accept(eventList, postgresOffset);
            PostgresOffsetStorage.postgresOffsetMap.put(runnerName, postgresOffset);
        }
    }

    private DataMap getMapFromStruct(Struct struct) {
        DataMap dataMap = new DataMap();
        if (EmptyKit.isNull(struct)) {
            return dataMap;
        }
        struct.schema().fields().forEach(field -> {
            Object obj = struct.getWithoutDefault(field.name());
            if (obj instanceof ByteBuffer) {
                obj = struct.getBytes(field.name());
            } else if (obj instanceof Struct) {
                obj = BigDecimal.valueOf(NumberKit.bytes2long(((Struct) obj).getBytes("value")), (int) ((Struct) obj).get("scale"));
            }
            dataMap.put(field.name(), obj);
        });
        return dataMap;
    }
}

