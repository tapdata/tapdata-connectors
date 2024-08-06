package io.tapdata.connector.postgres.partition.wrappper;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.type.TapPartitionList;
import io.tapdata.entity.schema.partition.type.TapPartitionType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ListWrapper extends PGPartitionWrapper {
    public static final String REGEX = "\\(([^()]+)\\)";


    //FOR VALUES IN (1,2,3)
    //FOR VALUES IN (4)
    //FOR VALUES IN (('North', 'Electronics'), ('South', 'Furniture'))
    //DEFAULT

    @Override
    public TapPartitionType.Type type() {
        return TapPartitionType.Type.LIST;
    }

    protected List<String[]> values(String check) {
        List<String[]> valuesList = new ArrayList<>();
        Pattern pattern = Pattern.compile(REGEX);
        Matcher matcher = pattern.matcher(check);
        while (matcher.find()) {
            String values = matcher.group(1);
            String[] fields = values.split(SPLIT_REGEX);
            valuesList.add(fields);
        }
        return valuesList;
    }

    @Override
    public List<? extends TapPartitionType> parse(TapTable table, String partitionSQL, String checkOrPartitionRule, Log log) {
        ArrayList<TapPartitionList<String>> lists = new ArrayList<>();
        if ("DEFAULT".equalsIgnoreCase(partitionSQL)) {
            TapPartitionList<String> stringTapPartitionList = new TapPartitionList<>(TapPartitionType.FieldType.STRING);
            stringTapPartitionList.setToDefault();
            lists.add(stringTapPartitionList);
            return lists;
        }
        List<String[]> values = values(partitionSQL);
        if (values.isEmpty()) {
            throw new CoreException("Unable find any value in LIST partition table: {}, partition SQL: {}", table.getId(), partitionSQL);
        }
        for (String[] value : values) {
            TapPartitionList<String> stringTapPartitionList = new TapPartitionList<>(TapPartitionType.FieldType.STRING);
            stringTapPartitionList.dataIn(new ArrayList<>(Arrays.asList(value)));
            lists.add(stringTapPartitionList);
        }
        return lists;
    }
}
