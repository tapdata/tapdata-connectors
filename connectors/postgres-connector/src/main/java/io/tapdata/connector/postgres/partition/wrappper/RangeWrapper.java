package io.tapdata.connector.postgres.partition.wrappper;

import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.type.TapPartitionRange;
import io.tapdata.entity.schema.partition.type.TapPartitionStage;
import io.tapdata.entity.schema.partition.type.TapPartitionType;
import io.tapdata.entity.schema.partition.type.TapRangeValue;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RangeWrapper extends PGPartitionWrapper {
    public static final String REGEX = "FOR VALUES FROM \\(([^)]+)\\) TO \\(([^)]+)\\)";
    public static final String MINVALUE = "MINVALUE";
    public static final String MAXVALUE = "MAXVALUE";
    //FOR VALUES FROM (MINVALUE) TO (100)
    //FOR VALUES FROM (100) TO (200)
    //FOR VALUES FROM (200) TO (MAXVALUE)

    @Override
    public List<TapPartitionType> parse(TapTable table, String partitionSQL, String checkOrPartitionRule, Log log) {
        partitionSQL = String.valueOf(partitionSQL).trim().toUpperCase();
        Pattern pattern = Pattern.compile(REGEX);
        Matcher matcher = pattern.matcher(partitionSQL);
        if (matcher.find()) {
            List<TapPartitionType> partitionRanges = new ArrayList<>();
            String[] fromValues = String.valueOf(matcher.group(1)).split(",");
            String[] toValues = String.valueOf(matcher.group(2)).split(",");
            if (fromValues.length != toValues.length) {
                log.warn("Cant not wrapper from and to value to set Range Partition info, partition sql: {}", partitionSQL);
                return null;
            }
            for (int index = 0; index < fromValues.length; index++) {
                String from = String.valueOf(fromValues[index]).trim();
                String to = String.valueOf(toValues[index]).trim();
                partitionRanges.add(new TapPartitionRange<String>()
                        .from(new TapRangeValue<>(filter(from, log), valueType(from.toUpperCase()), from))
                        .to(new TapRangeValue<>(filter(to, log), valueType(to.toUpperCase()), to))
                );
            }
            return partitionRanges;
        }
        log.warn("Cant not wrapper from and to value to set Range Partition info, partition sql: {}", partitionSQL);
        return null;
    }

    protected String filter(String value, Log log) {
        try {
            Pattern pattern = Pattern.compile("^'\\d{4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2}[+-]\\d{2}'$");
            Matcher matcher = pattern.matcher(value);
            if (matcher.find()) {
                value = value.substring(1, value.length() - 1);
                String timestamp = value.substring(0, value.length() - 3);
                String timezone = value.substring(value.length() - 3);
                String format = (Timestamp.valueOf(timestamp).toLocalDateTime().minusHours(Long.parseLong(timezone)))
                        .format(new DateTimeFormatterBuilder()
                                .appendPattern("yyyy-MM-dd HH:mm:ss")
                                .optionalStart()
                                .optionalEnd()
                                .toFormatter());
                return String.format("'%s'", format);
            }
        } catch (Exception e) {
            log.warn("Cant not wrapper time with time zone value to set Range Partition info, origin time format value: {}", value);
        }
        return value;
    }

    protected TapRangeValue.ValueType valueType(String value) {
        switch (value) {
            case MAXVALUE:
                return TapRangeValue.ValueType.MAX;
            case MINVALUE:
                return TapRangeValue.ValueType.MIN;
            default:
                return TapRangeValue.ValueType.NORMAL;
        }
    }

    @Override
    public TapPartitionStage type() {
        return TapPartitionStage.RANGE;
    }
}
