package io.tapdata.connector.tdengine;

import io.tapdata.connector.tdengine.config.TDengineConfig;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class TDengineSuperRecordWriter {

    private final TDengineJdbcContext tDengineJdbcContext;
    private final TapTable tapTable;
    private final TDengineConfig tDengineConfig;
    private final List<String> tags;
    private final List<String> otherColumns;

    public TDengineSuperRecordWriter(TDengineJdbcContext tDengineJdbcContext, TapTable tapTable) {
        this.tDengineJdbcContext = tDengineJdbcContext;
        this.tapTable = tapTable;
        this.tDengineConfig = (TDengineConfig) tDengineJdbcContext.getConfig();
        this.tags = Optional.ofNullable(tDengineConfig.getSuperTableTags()).orElse((List<String>) Optional.ofNullable(tapTable.getTableAttr()).orElse(new HashMap<>()).get("tags"));
        this.otherColumns = tapTable.getNameFieldMap().keySet().stream().filter(key -> !tags.contains(key)).collect(Collectors.toList());
    }

    public void write(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> consumer, Supplier<Boolean> isAlive) throws Exception {
        try (
                Connection connection = tDengineJdbcContext.getConnection();
                Statement statement = connection.createStatement()
        ) {
            int inserted = 0;
            StringBuilder largeSql = new StringBuilder("INSERT INTO ");
            for (TapRecordEvent tapRecordEvent : tapRecordEvents) {
                if (!isAlive.get()) {
                    return;
                }
                if (tapRecordEvent instanceof TapInsertRecordEvent) {
                    largeSql.append("`").append(getSubTableName(tags, (TapInsertRecordEvent) tapRecordEvent)).append("`")
                            .append(" USING ").append(String.format("`%s`.`%s` (", tDengineConfig.getDatabase(), tapTable.getId()))
                            .append("`").append(String.join("`,`", tags)).append("`) TAGS (")
                            .append(tags.stream().map(tag -> object2String(((TapInsertRecordEvent) tapRecordEvent).getAfter().get(tag))).collect(Collectors.joining(",")));
                    if (inserted == 0) {
                        largeSql.append(") (`").append(String.join("`,`", otherColumns)).append("`");
                    }
                    largeSql.append(") VALUES (")
                            .append(otherColumns.stream().map(other -> object2String(((TapInsertRecordEvent) tapRecordEvent).getAfter().get(other))).collect(Collectors.joining(",")))
                            .append(") ");
                    inserted++;
                }
            }
            statement.execute(largeSql.toString());
            connection.commit();
            WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
            consumer.accept(listResult.insertedCount(inserted));
        }
    }

    protected String getSubTableName(List<String> tags, TapInsertRecordEvent tapRecordEvent) {
        StringBuilder subTableName = new StringBuilder();
        if (EmptyKit.isEmpty(tDengineConfig.getSuperTableTags())) {
            subTableName.append(tapTable.getId());
            tags.forEach(t -> subTableName.append("_").append(tapRecordEvent.getAfter().get(t)));
        } else if ("AutoHash".equals(tDengineConfig.getSubTableNameType())) {
            subTableName.append(tapTable.getId()).append("_");
            subTableName.append(StringKit.md5(tags.stream().map(tag -> String.valueOf(tapRecordEvent.getAfter().get(tag))).collect(Collectors.joining(","))));
        } else {
            String key = tDengineConfig.getSubTableSuffix();
            key = key.replace("${superTableName}", tapTable.getId());
            for (String column : tags) {
                key = key.replace("${" + column + "}", String.valueOf(tapRecordEvent.getAfter().get(column)));
            }
            subTableName.append(key);
        }
        return subTableName.toString();
    }

    public String object2String(Object obj) {
        String result;
        if (null == obj) {
            result = "null";
        } else if (obj instanceof String) {
            result = "'" + ((String) obj).replace("\\", "\\\\")
                    .replace("'", "\\'")
                    .replace("(", "\\(")
                    .replace(")", "\\)") + "'";
        } else if (obj instanceof Number) {
            result = obj.toString();
        } else if (obj instanceof Date) {
            result = "'" + dateFormat.format(obj) + "'";
        } else if (obj instanceof Instant) {
            result = "'" + LocalDateTime.ofInstant((Instant) obj, ZoneId.of("GMT")).format(dateTimeFormatter) + "'";
        } else if (obj instanceof byte[]) {
            String hexString = convertToHexString((byte[]) obj);
            return "X'" + hexString + "'";
        } else if (obj instanceof Boolean) {
            if ("true".equalsIgnoreCase(obj.toString())) {
                return "1";
            }
            return "0";
        } else {
            return "'" + obj + "'";
        }
        return result;
    }

    public String convertToHexString(byte[] toBeConverted) {
        if (toBeConverted == null) {
            throw new NullPointerException("Parameter to be converted can not be null");
        }

        char[] converted = new char[toBeConverted.length * 2];
        for (int i = 0; i < toBeConverted.length; i++) {
            byte b = toBeConverted[i];
            converted[i * 2] = HEX_CHARS[b >> 4 & 0x0F];
            converted[i * 2 + 1] = HEX_CHARS[b & 0x0F];
        }

        return String.valueOf(converted);
    }

    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    private static final char[] HEX_CHARS = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
}
