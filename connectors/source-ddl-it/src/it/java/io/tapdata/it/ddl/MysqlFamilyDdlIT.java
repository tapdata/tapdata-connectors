package io.tapdata.it.ddl;

import io.tapdata.connector.mysql.MysqlConnector;
import java.util.function.Supplier;

public abstract class MysqlFamilyDdlIT extends JdbcFamilyDdlIT {
    protected MysqlFamilyDdlIT(Supplier<? extends MysqlConnector> factory, String specification,
                              String engine, String module) {
        super(factory, specification, engine, module);
    }
}
