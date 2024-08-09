package io.tapdata.connector.postgres.partition.wrappper;

import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.type.TapPartitionRange;
import io.tapdata.entity.schema.partition.type.TapPartitionStage;
import io.tapdata.entity.schema.partition.type.TapPartitionType;
import io.tapdata.entity.schema.partition.type.TapRangeValue;

import java.util.ArrayList;
import java.util.List;
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
                        .from(new TapRangeValue<>(from, valueType(from.toUpperCase()), from))
                        .to(new TapRangeValue<>(to, valueType(to.toUpperCase()), to))
                );
            }
            return partitionRanges;
        }
        log.warn("Cant not wrapper from and to value to set Range Partition info, partition sql: {}", partitionSQL);
        return null;
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
