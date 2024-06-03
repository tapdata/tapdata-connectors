package io.tapdata.connector.tidb.cdc.process.analyse;

import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.common.ddl.ccj.CCJBaseDDLWrapper;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.common.ddl.wrapper.DDLWrapperConfig;
import io.tapdata.connector.tidb.cdc.process.ddl.entity.DDLObject;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;

import java.util.concurrent.atomic.AtomicReference;

public class AnalyseTapEventFromDDLObject implements AnalyseRecord<DDLObject, TapEvent> {
    private final KVReadOnlyMap<TapTable> tapTableMap;
    private static final DDLWrapperConfig DDL_WRAPPER_CONFIG = CCJBaseDDLWrapper.CCJDDLWrapperConfig.create().split("`");
    private final DDLParserType ddlParserType = DDLParserType.MYSQL_CCJ_SQL_PARSER;

    public AnalyseTapEventFromDDLObject(KVReadOnlyMap<TapTable> tapTableMap) {
        this.tapTableMap = tapTableMap;
    }

    @Override
    public TapEvent analyse(DDLObject record, AnalyseColumnFilter<DDLObject> filter) {
        AtomicReference<TapEvent> mysqlStreamEvents = new AtomicReference<>();
        String ddlSql = record.getQuery();
        try {
            DDLFactory.ddlToTapDDLEvent(
                    ddlParserType,
                    ddlSql,
                    DDL_WRAPPER_CONFIG,
                    tapTableMap,
                    tapDDLEvent -> {
                        mysqlStreamEvents.set(tapDDLEvent);
                        TapLogger.info("TAG", "Read DDL: " + ddlSql + ", about to be packaged as some event(s)");
                    }
            );
        } catch (Throwable e) {
            throw new CoreException("Handle TiDB ddl failed: {}, error message: {}", ddlSql, e.getMessage(), e);
        }
        return mysqlStreamEvents.get();
    }
}
