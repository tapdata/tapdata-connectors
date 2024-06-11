package io.tapdata.connector.tidb.ddl.ccj;

import io.tapdata.connector.mysql.ddl.ccj.MysqlAlterColumnAttrsDDLWrapper;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import net.sf.jsqlparser.statement.alter.Alter;

import java.util.function.Consumer;

public class TidbAlterColumnAttrsDDLWrapper extends MysqlAlterColumnAttrsDDLWrapper {
    @Override
    public void wrap(Alter ddl, KVReadOnlyMap<TapTable> tableMap, Consumer<TapDDLEvent> consumer) {
        super.wrap(ddl, tableMap, consumer);
    }
}
