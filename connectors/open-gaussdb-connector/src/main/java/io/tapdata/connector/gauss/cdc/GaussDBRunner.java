package io.tapdata.connector.gauss.cdc;

import com.huawei.opengauss.jdbc.PGProperty;
import com.huawei.opengauss.jdbc.jdbc.PgConnection;
import com.huawei.opengauss.jdbc.replication.LogSequenceNumber;
import com.huawei.opengauss.jdbc.replication.PGReplicationStream;
import com.huawei.opengauss.jdbc.replication.fluent.logical.ChainedLogicalStreamBuilder;
import io.debezium.connector.postgresql.TypeRegistry;
import io.debezium.engine.DebeziumEngine;
import io.tapdata.connector.gauss.cdc.logic.event.EventFactory;
import io.tapdata.connector.gauss.cdc.logic.event.transcation.discrete.LogicReplicationDiscreteImpl;
import io.tapdata.connector.gauss.core.GaussDBConfig;
import io.tapdata.connector.gauss.enums.CdcConstant;
import io.tapdata.connector.postgres.cdc.DebeziumCdcRunner;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.NumberKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class GaussDBRunner extends DebeziumCdcRunner {
    public static Map<String, CdcOffset> offsetMap = new ConcurrentHashMap<>(); //one slot one key
    private final GaussDBConfig gaussDBConfig;
    private CdcOffset offset;
    private int recordSize;
    private StreamReadConsumer consumer;
    private final AtomicReference<Throwable> throwableAtomicReference = new AtomicReference<>();
    //protected final TimeZone timeZone;
    private final String jdbcURL;
    private final Log log;
    private PGReplicationStream stream;
    private PgConnection conn;
    private String whiteTableList;
    private EventFactory<ByteBuffer> eventFactory;
    private TypeRegistry typeRegistry;
    private long waitTime;
    private Supplier<Boolean> supplier;

    public GaussDBRunner(GaussDBConfig config, Log log) throws SQLException {
        this.log = log;
        this.gaussDBConfig = config;
        jdbcURL = String.format("jdbc:opengauss://%s:%s/%s",
                gaussDBConfig.getHaHost(),
                gaussDBConfig.getHaPort(),
                gaussDBConfig.getDatabase());
    }



    public GaussDBRunner supplierIsAlive(Supplier<Boolean> supplier) {
        this.supplier = supplier;
        return this;
    }

    public GaussDBRunner useSlot(String slotName) {
        this.runnerName = slotName;
        return this;
    }

    public GaussDBRunner waitTime(long waitTime) {
        if (waitTime < CdcConstant.CDC_FLUSH_LOGIC_LSN_MIN || waitTime > CdcConstant.CDC_FLUSH_LOGIC_LSN_MAX) {
            waitTime = CdcConstant.CDC_FLUSH_LOGIC_LSN_DEFAULT;
        }
        this.waitTime = waitTime;
        return this;
    }

    public GaussDBRunner watch(List<String> observedTableList) {
        if (null != observedTableList && !observedTableList.isEmpty()) {
            whiteTableList = String.join(",", observedTableList);
        }
        return this;
    }

    public GaussDBRunner offset(Object offsetState) {
        if (EmptyKit.isNull(offsetState) || !(offsetState instanceof CdcOffset)) {
            offset = new CdcOffset();
        } else {
            this.offset = (CdcOffset) offsetState;
        }
        offsetMap.put(runnerName, offset);
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
        //PGProperty.LOGGER.set(properties, "Slf4JLogger");
        //PGProperty.SSL.set(properties, "false");
        //try {
        //    Class.forName("com.huawei.opengauss.jdbc.ssl.NonValidatingFactory");
        //} catch (Exception e) {
        //
        //}
        //PGProperty.SSL_FACTORY.set(properties, "com.huawei.opengauss.jdbc.ssl.NonValidatingFactory");
        conn = (PgConnection) DriverManager.getConnection(jdbcURL, properties);
        log.info("Replication connection init completed");
        ChainedLogicalStreamBuilder streamBuilder = conn.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(runnerName)
                .withSlotOption("include-xids", false)
                .withSlotOption("skip-empty-xacts", true)
                //.withStartPosition(null)
                .withSlotOption("parallel-decode-num", 10);//解码线程并行度
        if (null != offset.getLsn()) {
            Object lsn = offset.getLsn();
            if (lsn instanceof Number) {
                streamBuilder.withStartPosition(LogSequenceNumber.valueOf(((Number)lsn).longValue()));
            } else if (lsn instanceof LogSequenceNumber) {
                streamBuilder.withStartPosition((LogSequenceNumber)lsn);
            }
        }
        if (null != whiteTableList && !whiteTableList.isEmpty()) {
            streamBuilder.withSlotOption("white-table-list", whiteTableList); //白名单列表
            log.info("Tables: {} will be monitored in cdc white table list", whiteTableList);
        }
        //build PGReplicationStream
        stream = streamBuilder.withSlotOption("standby-connection", true) //强制备机解码
                .withSlotOption("decode-style", "b") //解码格式
                .withSlotOption("sending-batch", 1) //批量发送解码结果
                .withSlotOption("max-txn-in-memory", 100) //单个解码事务落盘内存阈值为100MB
                .withSlotOption("max-reorderbuffer-in-memory", 50) //正在处理的解码事务落盘内存阈值为
                //.withSlotOption("exclude-users", "userA") //不返回用户userA执行事务的逻辑日志
                .withSlotOption("include-user", true) //事务BEGIN逻辑日志携带用户名字
                .withSlotOption("enable-heartbeat", true) // 开启心跳日志
                .withSlotOption("include-xids", true)
                .withSlotOption("include-timestamp", true)
                .start();
        log.info("GaussDB logic replication stream init completed");
        //typeRegistry = new TypeRegistry(new PostgresConnection(Configuration.from(gaussDBConfig.getProperties()), true));
        eventFactory = LogicReplicationDiscreteImpl.instance(consumer, 100, offset, typeRegistry, supplier);
        log.info("GaussDB logic log parser init completed");
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
        long cdcInitTime = System.currentTimeMillis();
        try {
            while (null != supplier && supplier.get()) {
                ByteBuffer byteBuffer = stream.readPending();
                if (null == byteBuffer) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(5L);
                    } catch (Exception ignore) {
                        //
                    }
                    continue;
                }
                eventFactory.emit(byteBuffer, log);
                //如果需要flush lsn，根据业务实际情况调用以下接口
                long now = System.currentTimeMillis();
                if (now - cdcInitTime >= waitTime) {
                    LogSequenceNumber lastRecv = stream.getLastReceiveLSN();
                    stream.setFlushedLSN(lastRecv);
                    stream.forceUpdateStatus();
                    log.debug("Pushed a log sequence: [{}], time: {}", lastRecv.asString(), now);
                    cdcInitTime = now;
                }
            }
        } catch (Exception e) {
            log.warn("Cdc stop whit an error: {}", e.getMessage());
            this.getThrowable().set(e);
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
        return null != supplier && supplier.get();
    }

    /**
     * close cdc sync
     */
    @Override
    public void closeCdcRunner() {
        try {
            if (null != stream) stream.close();
        } catch (Exception e) {
            log.warn("Close gauss db logic replication stream fail, message: {}", e.getMessage());
        }
        try {
            if (null != conn) conn.close();
        } catch (Exception e) {
            log.warn("Close replication jdbc connection fail, message: {}", e.getMessage());
        }
        try {
            if (null != engine) engine.close();
        } catch (Exception e) {
            log.warn("Close cdc engine fail, message: {}", e.getMessage());
        }
    }

    @Override
    public void run() {
        startCdcRunner();
    }

    @Override
    public void consumeRecords(List<SourceRecord> sourceRecords, DebeziumEngine.RecordCommitter<SourceRecord> committer) {
        super.consumeRecords(sourceRecords, committer);
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

