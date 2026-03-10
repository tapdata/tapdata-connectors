package io.tapdata.connector.klustron.target;

import io.tapdata.connector.klustron.config.KunLunPgConfig;
import io.tapdata.connector.klustron.context.KunLunPgContext;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/3/6 15:04 Create
 * @description
 */
public abstract class KunLunWriter<T> {
    protected KunLunPgContext pgJdbcContext;
    protected String version;
    protected KunLunPgConfig ofPgConfig;

    //offset for Primary key sorting area reading
    protected Map<String, DataMap> writtenTableMap;
    protected static final String HAS_UNIQUE_INDEX = "HAS_UNIQUE_INDEX";
    protected static final String HAS_MULTI_UNIQUE_INDEX = "HAS_MULTI_UNIQUE_INDEX";
    protected static final String HAS_AUTO_INCR = "HAS_AUTO_INCR";
    protected static final String CANNOT_CLOSE_CONSTRAINT = "CANNOT_CLOSE_CONSTRAINT";
    //jdbc context for each relation datasource
    protected Log tapLogger;
    protected Map<String, Connection> transactionConnectionMap;
    protected boolean isTransaction = false;

    Supplier<Boolean> isAlive;
    Function<String, String> getSchemaAndTable;

    public T init(KunLunPgContext pgJdbcContext, String version, boolean isTransaction, Log tapLogger) {
        this.version = version;
        this.pgJdbcContext = pgJdbcContext;
        this.ofPgConfig = (KunLunPgConfig) pgJdbcContext.getConfig();
        this.isTransaction = isTransaction;
        this.tapLogger = tapLogger;
        return (T) this;
    }

    public T writtenTableMap(Map<String, DataMap> writtenTableMap) {
        this.writtenTableMap = writtenTableMap;
        return (T) this;
    }

    public T transactionConnectionMap(Map<String, Connection> transactionConnectionMap) {
        this.transactionConnectionMap = transactionConnectionMap;
        return (T) this;
    }

    public T isAlive(Supplier<Boolean> isAlive) {
        this.isAlive = isAlive;
        return (T) this;
    }

    public T getSchemaAndTable(Function<String, String> getSchemaAndTable) {
        this.getSchemaAndTable = getSchemaAndTable;
        return (T) this;
    }

    public abstract void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents, TapTable tapTable, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws SQLException;

}
