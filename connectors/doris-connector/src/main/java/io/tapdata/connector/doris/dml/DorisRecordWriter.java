package io.tapdata.connector.doris.dml;

import io.tapdata.common.JdbcContext;
import io.tapdata.common.dml.NormalRecordWriter;
import io.tapdata.connector.doris.bean.DorisConfig;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.entity.WriteListResult;

import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class DorisRecordWriter extends NormalRecordWriter {

    protected DorisStreamWriteRecorder streamRecorder;

    public DorisRecordWriter(JdbcContext jdbcContext, TapTable tapTable) throws SQLException {
        super(jdbcContext, tapTable);
    }

    public void write(List<TapRecordEvent> tapRecordEvents, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer, Supplier<Boolean> isAlive) throws SQLException {
        WriteListResult<TapRecordEvent> listResult = new WriteListResult<>();
        for (TapRecordEvent recordEvent : tapRecordEvents) {
            if (null != isAlive && !isAlive.get()) {
                break;
            }
            if (recordEvent instanceof TapInsertRecordEvent) {
                //插入事件，全部可以使用导入模式
                streamRecorder.addInsertBatch(((TapInsertRecordEvent) recordEvent).getAfter(), listResult);
            } else if (recordEvent instanceof TapUpdateRecordEvent) {
                //更新事件，需要考虑更多的因素
                if (Boolean.FALSE.equals(((DorisConfig) commonDbConfig).getJdbcCompletion())) {
                    //未开启jdbc补全，只能使用导入模式
                    streamRecorder.addUpdateBatch(((TapUpdateRecordEvent) recordEvent).getAfter(), ((TapUpdateRecordEvent) recordEvent).getBefore(), listResult);
                } else {
                    updateRecorder.addUpdateBatch(((TapUpdateRecordEvent) recordEvent).getAfter(), ((TapUpdateRecordEvent) recordEvent).getBefore(), listResult);
                }
            }
        }
    }
}
