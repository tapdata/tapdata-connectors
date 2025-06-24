package io.tapdata.connector.postgres.cdc;

import com.alibaba.fastjson.JSONObject;
import io.tapdata.base.ConnectorBase;
import io.tapdata.common.concurrent.ConcurrentProcessor;
import io.tapdata.common.concurrent.TapExecutors;
import io.tapdata.common.sqlparser.ResultDO;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.HttpKit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class WalPgtoMiner extends AbstractWalLogMiner {

    private String connectorId;

    public WalPgtoMiner(PostgresJdbcContext postgresJdbcContext, String connectorId, Log tapLogger) {
        super(postgresJdbcContext, tapLogger);
        this.connectorId = connectorId;
    }

    @Override
    public AbstractWalLogMiner offset(Object offsetState) {
        return this;
    }

    private final static String WALMINER_ADD_SUB = "ADDSUB:%s %s.%s.%s";

    @Override
    public void startMiner(Supplier<Boolean> isAlive) throws Throwable {
        for (String table : tableList) {
            HttpKit.sendHttp09Request(postgresConfig.getPgtoHost(), postgresConfig.getPgtoPort(), String.format(WALMINER_ADD_SUB, connectorId, postgresConfig.getDatabase(), postgresConfig.getSchema(), table));
        }
        try (ConcurrentProcessor<String, NormalRedo> concurrentProcessor = TapExecutors.createSimple(8, 32, "wal-miner")) {
            Thread t = new Thread(() -> {
                consumer.streamReadStarted();
                NormalRedo lastRedo = null;
                AtomicReference<List<TapEvent>> events = new AtomicReference<>(ConnectorBase.list());
                while (isAlive.get()) {
                    try {
                        NormalRedo redo = concurrentProcessor.get(2, TimeUnit.SECONDS);
                        if (EmptyKit.isNotNull(redo)) {
                            if (EmptyKit.isNotNull(redo.getOperation())) {
                                lastRedo = redo;
                                events.get().add(createEvent(redo));
                                if (events.get().size() >= recordSize) {
                                    consumer.accept(events.get(), redo.getCdcSequenceStr());
                                    events.set(new ArrayList<>());
                                }
                            } else {
                                consumer.accept(Collections.singletonList(new HeartbeatEvent().init().referenceTime(System.currentTimeMillis())), redo.getCdcSequenceStr());
                            }
                        } else {
                            if (!events.get().isEmpty()) {
                                consumer.accept(events.get(), lastRedo.getCdcSequenceStr());
                                events.set(new ArrayList<>());
                            }
                        }
                    } catch (Exception e) {
                        threadException.set(e);
                    }
                }
            });
            t.setName("wal-miner-Consumer");
            t.start();
            while (isAlive.get()) {
                if (EmptyKit.isNotNull(threadException.get())) {
                    consumer.streamReadEnded();
                    throw new RuntimeException(threadException.get());
                }
                String json = HttpKit.sendHttp09Request(postgresConfig.getPgtoHost(), postgresConfig.getPgtoPort(), "SUB:" + connectorId + " seek");
                extractJsonObjects(json).forEach(str -> concurrentProcessor.runAsync(str, r -> {
                    try {
                        return emit(r);
                    } catch (Throwable e) {
                        threadException.set(e);
                    }
                    return null;
                }));
            }
        } finally {
            consumer.streamReadEnded();
        }
    }

    private NormalRedo emit(String json) {
        JSONObject jsonObject = TapSimplify.fromJson(json, JSONObject.class);
        String sql = jsonObject.getString("sql");
        ResultDO resultDO = sqlParser.from(sql, false);
        if (EmptyKit.isNull(resultDO)) {
            return null;
        }
        String schema = jsonObject.getString("schemaname");
        String tableName = jsonObject.getString("relname");
        String op;
        switch (resultDO.getOp()) {
            case INSERT:
                op = "1";
                break;
            case UPDATE:
                op = "2";
                break;
            case DELETE:
                op = "3";
                break;
            default:
                op = null;
        }
        NormalRedo redo = new NormalRedo();
        redo.setSqlRedo(sql);
        redo.setOperation(op);
        for (Map.Entry<String, Object> entry : resultDO.getData().entrySet()) {
            parseKeyAndValue(tableName, entry);
        }
        redo.setRedoRecord(resultDO.getData());
        redo.setNameSpace(schema);
        redo.setTableName(tableName);
        redo.setTimestamp(System.currentTimeMillis());
        redo.setTransactionId(jsonObject.getString("xid"));
        redo.setCdcSequenceStr(jsonObject.getString("lsn"));
        return redo;
    }

    private List<String> extractJsonObjects(String input) {
        List<String> jsonObjects = new ArrayList<>();
        StringBuilder jsonBuilder = null;
        int braceCount = 0;
        boolean insideJson = false;
        boolean insideString = false;
        boolean escaped = false;

        char[] chars = input.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            char c = chars[i];

            // 处理转义字符
            if (escaped) {
                if (insideJson) {
                    jsonBuilder.append(c);
                }
                escaped = false;
                continue;
            }

            // 检查转义字符
            if (c == '\\') {
                if (insideJson) {
                    jsonBuilder.append(c);
                }
                escaped = true;
                continue;
            }

            // 处理字符串边界
            if (c == '"') {
                if (insideJson) {
                    jsonBuilder.append(c);
                }
                insideString = !insideString;
                continue;
            }

            // 只有在字符串外部才处理大括号
            if (!insideString) {
                if (c == '{') {
                    if (!insideJson) {
                        // 开始新的JSON对象
                        jsonBuilder = new StringBuilder();
                        insideJson = true;
                    }
                    braceCount++;
                }

                if (insideJson) {
                    jsonBuilder.append(c);

                    if (c == '}') {
                        braceCount--;
                        // 当所有大括号都闭合时
                        if (braceCount == 0) {
                            jsonObjects.add(jsonBuilder.toString());
                            insideJson = false;
                            insideString = false; // 重置字符串状态
                        }
                    }
                }
            } else {
                // 在字符串内部，直接添加字符
                if (insideJson) {
                    jsonBuilder.append(c);
                }
            }
        }

        return jsonObjects;
    }
}
