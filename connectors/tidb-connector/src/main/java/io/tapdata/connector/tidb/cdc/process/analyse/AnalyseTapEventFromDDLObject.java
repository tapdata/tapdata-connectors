package io.tapdata.connector.tidb.cdc.process.analyse;

import io.tapdata.common.ddl.DDLFactory;
import io.tapdata.common.ddl.ccj.CCJBaseDDLWrapper;
import io.tapdata.common.ddl.type.DDLParserType;
import io.tapdata.common.ddl.wrapper.DDLWrapperConfig;
import io.tapdata.connector.tidb.cdc.process.ddl.entity.DDLObject;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.consumer.StreamReadOneByOneConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AnalyseTapEventFromDDLObject implements AnalyseRecord<DDLObject, List<TapEvent>> {
    private final KVReadOnlyMap<TapTable> tapTableMap;
    private static final DDLWrapperConfig DDL_WRAPPER_CONFIG = CCJBaseDDLWrapper.CCJDDLWrapperConfig.create().split("`");
    StreamReadOneByOneConsumer consumer;
    Map<String, Map<String, Long>> offset;

    public AnalyseTapEventFromDDLObject(KVReadOnlyMap<TapTable> tapTableMap) {
        this.tapTableMap = tapTableMap;
    }

    public AnalyseTapEventFromDDLObject withConsumer(StreamReadOneByOneConsumer consumer, Map<String, Map<String, Long>> offset) {
        this.consumer = consumer;
        this.offset = offset;
        return this;
    }

    @Override
    public List<TapEvent> analyse(DDLObject ddlObject, AnalyseColumnFilter<DDLObject> filter, Log log) {
        List<TapEvent> mysqlStreamEvents = new ArrayList<>();
        String ddlSql = ddlObject.getQuery();
        try {
            DDLFactory.ddlToTapDDLEvent(
                    DDLParserType.TIDB_CCJ_SQL_PARSER,
                    ddlSql,
                    DDL_WRAPPER_CONFIG,
                    tapTableMap,
                    tapDDLEvent -> {
                        tapDDLEvent.setTime(System.currentTimeMillis());
                        this.consumer.accept(tapDDLEvent, offset);
                        log.debug("Read DDL: {}, about to be packaged as some event(s)", ddlSql);
                    }
            );
        } catch (Throwable e) {
            throw new CoreException("Handle TiDB ddl failed: {}, error message: {}", ddlSql, e.getMessage(), e);
        }
        return mysqlStreamEvents;
    }
}
