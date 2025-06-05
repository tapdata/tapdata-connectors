package io.tapdata.connector.postgres.cdc;

import com.alibaba.fastjson.JSONObject;
import io.tapdata.common.sqlparser.ResultDO;
import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.cdc.offset.PostgresOffset;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.HttpKit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static io.tapdata.entity.simplify.TapSimplify.list;

public class WalPgtoMiner extends AbstractWalLogMiner {

    public WalPgtoMiner(PostgresJdbcContext postgresJdbcContext, Log tapLogger) {
        super(postgresJdbcContext, tapLogger);
    }

    @Override
    public AbstractWalLogMiner offset(Object offsetState) {
        return this;
    }

    @Override
    public void startMiner(Supplier<Boolean> isAlive) throws Throwable {
        while (isAlive.get()) {
            String json = HttpKit.sendHttp09Request(postgresConfig.getPgtoHost(), postgresConfig.getPgtoPort(), "seek");
            AtomicReference<List<TapEvent>> tapEvents = new AtomicReference<>(list());
            extractJsonObjects(json).forEach(str -> {
                JSONObject jsonObject = TapSimplify.fromJson(str, JSONObject.class);
                String sql = jsonObject.getString("sql");
                ResultDO resultDO = sqlParser.from(sql, false);
                if (EmptyKit.isNull(resultDO)) {
                    return;
                }
                String schema = resultDO.getSchema();
                String tableName = resultDO.getTableName();
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
                if (!postgresConfig.getSchema().equals(schema) || !tableList.contains(tableName)) {
                    return;
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
                tapEvents.get().add(createEvent(redo));
                if (tapEvents.get().size() >= recordSize) {
                    consumer.accept(tapEvents.get(), new PostgresOffset());
                    tapEvents.set(new ArrayList<>());
                }
            });
            if (EmptyKit.isNotEmpty(tapEvents.get())) {
                consumer.accept(tapEvents.get(), new PostgresOffset());
            }
        }
    }

    private List<String> extractJsonObjects(String input) {
        List<String> jsonObjects = new ArrayList<>();
        StringBuilder jsonBuilder = null;
        int braceCount = 0;
        boolean insideJson = false;

        for (char c : input.toCharArray()) {
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
                    }
                }
            }
        }

        return jsonObjects;
    }
}
