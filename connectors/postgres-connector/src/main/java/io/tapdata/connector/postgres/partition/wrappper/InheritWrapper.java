package io.tapdata.connector.postgres.partition.wrappper;

import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.type.TapPartitionType;

import java.util.List;

public class InheritWrapper extends PGPartitionWrapper {

    @Override
    public TapPartitionType.Type type() {
        return TapPartitionType.Type.INHERIT;
    }

    @Override
    public List<? extends TapPartitionType> parse(TapTable table, String partitionSQL, String checkOrPartitionRule, Log log) {
        return null;
    }
}
