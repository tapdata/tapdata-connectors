package io.tapdata.it.ddl;

import io.tapdata.base.ConnectorBase;
import java.util.function.Supplier;

public abstract class MysqlFamilyDdlIT extends JdbcFamilyDdlIT {
    protected MysqlFamilyDdlIT(Supplier<? extends ConnectorBase> factory, String specification,
                              String engine, String module) {
        super(factory, specification, engine, module);
    }
}
