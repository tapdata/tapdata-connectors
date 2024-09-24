package io.tapdata.kafka.serialization;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.events.EventOperation;
import io.tapdata.kafka.utils.KafkaUtils;
import io.tapdata.kafka.utils.StandardEventUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

/**
 * TapData 标准事件序列器
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/2 17:51 Create
 */
public class StandardSerializer implements Serializer<TapEvent> {

    @Override
    public byte[] serialize(String topic, TapEvent data) {
        Map<String, Object> map = new HashMap<>();
        if (data instanceof TapRecordEvent) {
            if (data instanceof TapInsertRecordEvent) {
                toDmlInsertMap(map, (TapInsertRecordEvent) data);
            } else if (data instanceof TapUpdateRecordEvent) {
                toDmlUpdateMap(map, (TapUpdateRecordEvent) data);
            } else if (data instanceof TapDeleteRecordEvent) {
                toDmlDeleteMap(map, (TapDeleteRecordEvent) data);
            } else if (data instanceof TapUnknownRecordEvent) {
                toDmlUnknownMap(map, (TapUnknownRecordEvent) data);
            } else {
                toUnknownMap(map, data);
            }
        } else {
            toUnknownMap(map, data);
        }
        return KafkaUtils.toJsonBytes(map);
    }

    private void fillTapEventInfo(Map<String, Object> map, TapEvent event) {
        StandardEventUtils.setTs(map, event.getTime());
    }

    private void fillTapRecordEventInfo(Map<String, Object> map, TapRecordEvent recordEvent) {
        fillTapEventInfo(map, recordEvent);
        StandardEventUtils.setOpTs(map, recordEvent.getReferenceTime());
        StandardEventUtils.setTable(map, recordEvent.getTableId());
        StandardEventUtils.setNamespaces(map, recordEvent.getNamespaces());
    }

    private void toDmlInsertMap(Map<String, Object> map, TapInsertRecordEvent recordEvent) {
        fillTapRecordEventInfo(map, recordEvent);
        StandardEventUtils.setOp(map, EventOperation.DML_INSERT);
        StandardEventUtils.setAfter(map, recordEvent.getAfter());
    }

    private void toDmlUpdateMap(Map<String, Object> map, TapUpdateRecordEvent recordEvent) {
        StandardEventUtils.setOp(map, EventOperation.DML_UPDATE);
        StandardEventUtils.setAfter(map, recordEvent.getAfter());
        StandardEventUtils.setBefore(map, recordEvent.getBefore());
    }

    private void toDmlDeleteMap(Map<String, Object> map, TapDeleteRecordEvent recordEvent) {
        StandardEventUtils.setOp(map, EventOperation.DML_DELETE);
        StandardEventUtils.setBefore(map, recordEvent.getBefore());
    }

    private void toDmlUnknownMap(Map<String, Object> map, TapUnknownRecordEvent recordEvent) {
        StandardEventUtils.setOp(map, EventOperation.DML_UNKNOWN);
        StandardEventUtils.setData(map, recordEvent.getData());
    }

    private void toUnknownMap(Map<String, Object> map, TapEvent recordEvent) {
        StandardEventUtils.setOp(map, EventOperation.DML_UNKNOWN);
        StandardEventUtils.setData(map, recordEvent);
    }
}
