package io.tapdata.connector.tidb.ddl.ccj;

import io.tapdata.connector.mysql.ddl.ccj.MysqlAlterColumnNameDDLWrapper;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.entity.ValueChange;
import io.tapdata.entity.event.ddl.table.TapAlterFieldNameEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import io.tapdata.pdk.apis.entity.Capability;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import net.sf.jsqlparser.statement.alter.Alter;
import net.sf.jsqlparser.statement.alter.AlterExpression;
import net.sf.jsqlparser.statement.alter.AlterOperation;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public class TidbAlterColumnNameDDLWrapper extends MysqlAlterColumnNameDDLWrapper {

    @Override
    public List<Capability> getCapabilities() {
        return super.getCapabilities();
    }

    @Override
    public void wrap(Alter ddl, KVReadOnlyMap<TapTable> tableMap, Consumer<TapDDLEvent> consumer) {
        super.wrap(ddl, tableMap, consumer);
    }
}
