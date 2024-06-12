package io.tapdata.connector.gauss.cdc;

import com.huawei.opengauss.jdbc.PGProperty;
import com.huawei.opengauss.jdbc.jdbc.PgConnection;
import com.huawei.opengauss.jdbc.replication.LogSequenceNumber;
import com.huawei.opengauss.jdbc.replication.PGReplicationConnection;
import com.huawei.opengauss.jdbc.replication.PGReplicationStream;
import com.huawei.opengauss.jdbc.replication.fluent.ChainedStreamBuilder;
import com.huawei.opengauss.jdbc.replication.fluent.logical.ChainedLogicalStreamBuilder;
import io.debezium.embedded.EmbeddedEngine;
import io.tapdata.connector.gauss.cdc.logic.event.EventFactory;
import io.tapdata.connector.gauss.cdc.logic.event.transcation.discrete.LogicReplicationDiscreteImpl;
import io.tapdata.connector.gauss.core.GaussDBConfig;
import io.tapdata.connector.gauss.enums.CdcConstant;
import io.tapdata.connector.postgres.cdc.DebeziumCdcRunner;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

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

import static io.tapdata.base.ConnectorBase.list;

public class GaussDBRunner extends DebeziumCdcRunner {
    protected static Map<String, CdcOffset> offsetMap = new ConcurrentHashMap<>(); //one slot one key
    protected GaussDBConfig gaussDBConfig;
    protected CdcOffset offset;
    private int recordSize;
    protected StreamReadConsumer consumer;
    protected final AtomicReference<Throwable> throwableAtomicReference = new AtomicReference<>();
    //protected final TimeZone timeZone;
    protected String jdbcURL;
    protected Log log;
    protected PGReplicationStream stream;
    protected PgConnection conn;
    protected String whiteTableList;
    protected EventFactory<ByteBuffer> eventFactory;
    protected long waitTime;
    protected Supplier<Boolean> supplier;
    public static GaussDBRunner instance() {
        return new GaussDBRunner();
    }

    public GaussDBRunner init(GaussDBConfig config, Log log) {
        if (null == config) {
            throw new CoreException("Failed init GaussDB cdc runner, message: GaussDB config must not be empty");
        }
        this.log = log;
        this.gaussDBConfig = config;
        final String haHost = config.getHaHost();
        final int haPort = config.getHaPort();
        final String database = config.getDatabase();
        this.jdbcURL = String.format("jdbc:opengauss://%s:%s/%s", haHost, haPort, database);
        return this;
    }

    public void registerConsumer(StreamReadConsumer consumer, int recordSize) throws SQLException {
        this.recordSize = recordSize;
        this.consumer = consumer;
        try {
            setPGReplicationStream(false);
        } catch (Exception e) {
            if (null != this.conn) {
                this.conn.close();
            }
            setPGReplicationStream(true);
        }
        this.log.info("GaussDB logic replication stream init completed");
        this.eventFactory = initEventFactory();
        this.log.info("GaussDB logic log parser init completed");
    }

    protected void setPGReplicationStream(boolean isCloud) throws SQLException {
        Properties properties = initProperties();
        this.conn = initCdcConnection(properties);
        this.log.info("Replication connection init completed");
        ChainedLogicalStreamBuilder streamBuilder = initChainedLogicalStreamBuilder();
        initCdcOffset(streamBuilder);
        initWhiteTableList(streamBuilder);
        this.stream = initPGReplicationStream(streamBuilder, isCloud);
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
                if (verifyByteBuffer(byteBuffer)) {
                    eventFactory.emit(byteBuffer, log);
                    cdcInitTime = flushLsn(cdcInitTime);

                    HeartbeatEvent event = new HeartbeatEvent();
                    long now = System.currentTimeMillis();
                    event.referenceTime(now);
                    event.setTime(now);
                    consumer.accept(list(event), offset);
                }
            }
        } catch (Exception e) {
            log.warn("CDC stop with an error: {}", e.getMessage());
            this.getThrowable().set(e);
        } finally {
            consumer.streamReadEnded();
            closeCdcRunner();
        }
    }

    protected boolean verifyByteBuffer(ByteBuffer byteBuffer) {
        if (null == byteBuffer) {
            try {
                sleep(5L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.debug(e.getMessage());
            }
            return false;
        }
        return true;
    }

    protected void sleep(long time) throws InterruptedException {
        TimeUnit.MILLISECONDS.sleep(time);
    }

    protected long flushLsn(long cdcInitTime) throws SQLException {
        //如果需要flush lsn，根据业务实际情况调用以下接口
        long now = System.currentTimeMillis();
        long valueTime = now - cdcInitTime;
        if (valueTime >= waitTime) {
            LogSequenceNumber lastRecv = stream.getLastReceiveLSN();
            stream.setFlushedLSN(lastRecv);
            stream.forceUpdateStatus();
            log.debug("Pushed a log sequence: [{}], time: {}", lastRecv.asString(), now);
            cdcInitTime = now;
        }
        return cdcInitTime;
    }

    @Override
    public boolean isRunning() {
        return null != supplier && Boolean.TRUE.equals(supplier.get());
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
        EmbeddedEngine engine = getEngine();
        try {
            if (null != engine) engine.close();
        } catch (Exception e) {
            log.warn("Close cdc engine fail, message: {}", e.getMessage());
        }
        offsetMap.remove(getRunnerName());
    }

    public EmbeddedEngine getEngine() {
        return engine;
    }

    @Override
    public void run() {
        startCdcRunner();
    }

    protected Properties initProperties() {
        if (null == gaussDBConfig) {
            throw new CoreException("Can not init cdc connection properties, message: GaussDB config can not be empty");
        }
        Properties properties = new Properties();
        //传递连接配置属性
        Properties dbConfigProperties = gaussDBConfig.getProperties();
        if (null != dbConfigProperties && !dbConfigProperties.isEmpty()) {
            properties.putAll(dbConfigProperties);
        }

        String user = gaussDBConfig.getUser();
        PGProperty.USER.set(properties, user);
        String password = gaussDBConfig.getPassword();
        PGProperty.PASSWORD.set(properties, password);
        // 对于逻辑复制，以下三个属性是必须配置项
        PGProperty.REPLICATION.set(properties, "database");
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "9.4");
        PGProperty.PREFER_QUERY_MODE.set(properties, "simple");
        String schema = gaussDBConfig.getSchema();
        PGProperty.CURRENT_SCHEMA.set(properties, schema);
        if (!PGProperty.SSL.getBoolean(properties)) {
            PGProperty.SSL.set(properties, "false");
        }
        //PGProperty.SSL_FACTORY.set(properties, "com.huawei.opengauss.jdbc.ssl.NonValidatingFactory");
        return properties;
    }

    protected PgConnection initCdcConnection(Properties properties) throws SQLException {
        if (null == jdbcURL) {
            throw new CoreException("Failed connect jdbc, message: jdbc url is empty");
        }
        if (null == properties || properties.isEmpty()) {
            throw new CoreException("Failed connect jdbc, message: jdbc properties is empty");
        }
        return (PgConnection) DriverManager.getConnection(jdbcURL, properties);
    }

    protected ChainedLogicalStreamBuilder initChainedLogicalStreamBuilder() {
        if (null == conn) {
            throw new CoreException("Failed start ChainedLogicalStream, message: GaussDB connection is empty");
        }
        PGReplicationConnection api = conn.getReplicationAPI();
        ChainedStreamBuilder streamBuilder = api.replicationStream();
        ChainedLogicalStreamBuilder logical = streamBuilder.logical();
        return logical.withSlotName(runnerName)
                .withSlotOption("include-xids", false)
                .withSlotOption("skip-empty-xacts", true)
                //.withStartPosition(null)
                .withSlotOption("parallel-decode-num", 10);//解码线程并行度
    }

    protected void initCdcOffset(ChainedLogicalStreamBuilder streamBuilder) {
        if (null == streamBuilder) return;
        if (null == offset) {
            return;
        }
        Object lsn = offset.getLsn();
        if (null == lsn) {
            return;
        }
        if (lsn instanceof LogSequenceNumber) {
            streamBuilder.withStartPosition((LogSequenceNumber)lsn);
            return;
        }
        if (lsn instanceof Number) {
            long longValue = ((Number) lsn).longValue();
            LogSequenceNumber logSequenceNumber = LogSequenceNumber.valueOf(longValue);
            streamBuilder.withStartPosition(logSequenceNumber);
        }
    }

    protected void initWhiteTableList(ChainedLogicalStreamBuilder streamBuilder) {
        if (null == streamBuilder) return;
        if (null != whiteTableList && !whiteTableList.isEmpty()) {
            streamBuilder.withSlotOption("white-table-list", whiteTableList); //白名单列表
            log.info("Tables: {} will be monitored in cdc white table list", whiteTableList);
        }
    }

    protected PGReplicationStream initPGReplicationStream(ChainedLogicalStreamBuilder streamBuilder, boolean isCloud) throws SQLException {
        if (null == streamBuilder) {
            throw new CoreException("Can not init GaussDB Replication Stream, message: Chained Logical Stream is empty");
        }
        streamBuilder = streamBuilder.withSlotOption("standby-connection", true);//强制备机解码
        streamBuilder = streamBuilder.withSlotOption("decode-style", "b");//解码格式
        streamBuilder = streamBuilder.withSlotOption("sending-batch", 1);//批量发送解码结果
        streamBuilder = streamBuilder.withSlotOption("max-txn-in-memory", 100); //单个解码事务落盘内存阈值为100MB
        streamBuilder = streamBuilder.withSlotOption("max-reorderbuffer-in-memory", 50); //正在处理的解码事务落盘内存阈值为
        //streamBuilder = streamBuilder.withSlotOption("exclude-users", "userA") //不返回用户userA执行事务的逻辑日志
        streamBuilder = streamBuilder.withSlotOption("include-user", true); //事务BEGIN逻辑日志携带用户名字

        //云数据库不支持
        if (!isCloud) {
            streamBuilder = streamBuilder.withSlotOption("enable-heartbeat", true); // 开启心跳日志
        }

        streamBuilder = streamBuilder.withSlotOption("include-xids", true);
        streamBuilder = streamBuilder.withSlotOption("include-timestamp", true);
        PGReplicationStream start = streamBuilder.start();
        return start;
    }

    protected EventFactory<ByteBuffer> initEventFactory() {
        return LogicReplicationDiscreteImpl.instance(consumer, 100, offset, supplier);
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
            this.offset = CdcOffset.fromOffset(offsetState);
        } else {
            this.offset = (CdcOffset) offsetState;
        }
        offsetMap.put(getRunnerName(), offset);
        return this;
    }

    public AtomicReference<Throwable> getThrowable() {
        return throwableAtomicReference;
    }
}

