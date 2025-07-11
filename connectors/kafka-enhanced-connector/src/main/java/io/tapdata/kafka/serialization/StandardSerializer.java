package io.tapdata.kafka.serialization;

import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.events.EventOperation;
import io.tapdata.kafka.utils.KafkaUtils;
import io.tapdata.kafka.utils.StandardEventUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.util.*;

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
        StandardEventUtils.setTs(map, data.getTime());
        if (data instanceof TapBaseEvent) {
            TapBaseEvent recordEvent = (TapBaseEvent) data;
            StandardEventUtils.setOpTs(map, null != recordEvent.getReferenceTime() ? recordEvent.getReferenceTime() : System.currentTimeMillis());
            StandardEventUtils.setTable(map, recordEvent.getTableId());
            StandardEventUtils.setNamespaces(map, getNamespaces(recordEvent));
        } else {
            StandardEventUtils.setTable(map, topic);
        }

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

    private void toDmlInsertMap(Map<String, Object> map, TapInsertRecordEvent recordEvent) {
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

    private List<String> getNamespaces(TapBaseEvent tapBaseEvent) {
        if (null == tapBaseEvent) {
            return null;
        }
        List<String> namespaces = tapBaseEvent.getNamespaces();
        if (null != namespaces) {
            return namespaces;
        }
        List<String> ns = new ArrayList<>();
        String database = tapBaseEvent.getDatabase();
        if (null != database) {
            ns.add(database);
        }
        String schema = tapBaseEvent.getSchema();
        if (null != schema) {
            ns.add(schema);
        }
        if (!ns.isEmpty()) {
            return ns;
        }
        return null;
    }
}
