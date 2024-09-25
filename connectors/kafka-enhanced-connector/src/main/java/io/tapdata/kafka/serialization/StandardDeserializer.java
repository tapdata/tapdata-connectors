package io.tapdata.kafka.serialization;

import com.alibaba.fastjson.JSONObject;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.events.EventOperation;
import io.tapdata.kafka.utils.KafkaUtils;
import io.tapdata.kafka.utils.StandardEventUtils;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * TapData 标准事件反序列器
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/2 17:51 Create
 */
public class StandardDeserializer implements Deserializer<TapEvent> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public TapEvent deserialize(String topic, byte[] bytes) {
        if (null == bytes) return null;

        try {
            JSONObject json = KafkaUtils.parseJsonObject(bytes);
            EventOperation op = StandardEventUtils.getOp(json);
            if (null == op) return toTapUnknownRecordEvent(topic, bytes);

            switch (op) {
                case DML_INSERT:
                    return toTapInsertRecordEvent(topic, json);
                case DML_UPDATE:
                    return toTapTapUpdateRecordEvent(topic, json);
                case DML_DELETE:
                    return toTapDeleteRecordEvent(topic, json);
                case DML_UNKNOWN:
                    return toTapUnknownRecordEvent(topic, json);
                default:
                    return toTapUnknownRecordEvent(topic, bytes);
            }
        } catch (Exception e) {
            return toTapUnknownRecordEvent(topic, bytes);
        }
    }

    private TapInsertRecordEvent toTapInsertRecordEvent(String topic, JSONObject json) {
        TapInsertRecordEvent recordEvent = TapInsertRecordEvent.create();
        recordEvent.setAfter(StandardEventUtils.getAfter(json));
        fillTapRecordEventInfo(topic, json, recordEvent);
        return recordEvent;
    }

    private TapUpdateRecordEvent toTapTapUpdateRecordEvent(String topic, JSONObject json) {
        TapUpdateRecordEvent recordEvent = TapUpdateRecordEvent.create();
        recordEvent.setBefore(StandardEventUtils.getBefore(json));
        recordEvent.setAfter(StandardEventUtils.getAfter(json));
        fillTapRecordEventInfo(topic, json, recordEvent);
        return recordEvent;
    }

    private TapDeleteRecordEvent toTapDeleteRecordEvent(String topic, JSONObject json) {
        TapDeleteRecordEvent recordEvent = TapDeleteRecordEvent.create();
        recordEvent.setBefore(StandardEventUtils.getBefore(json));
        fillTapRecordEventInfo(topic, json, recordEvent);
        return recordEvent;
    }

    private TapUnknownRecordEvent toTapUnknownRecordEvent(String topic, JSONObject json) {
        TapUnknownRecordEvent recordEvent = TapUnknownRecordEvent.create();
        recordEvent.setData(StandardEventUtils.getData(json));
        fillTapRecordEventInfo(topic, json, recordEvent);
        return recordEvent;
    }

    private TapUnknownRecordEvent toTapUnknownRecordEvent(String topic, byte[] bytes) {
        TapUnknownRecordEvent recordEvent = TapUnknownRecordEvent.create();
        recordEvent.setTableId(topic);
        recordEvent.setReferenceTime(System.currentTimeMillis());
        try {
            String data = new String(bytes);
            return recordEvent.value(data);
        } catch (Exception e) {
            return recordEvent.value(bytes);
        }
    }

    private void fillTapRecordEventInfo(String topic, JSONObject json, TapRecordEvent event) {
        event.setReferenceTime(StandardEventUtils.getOpTs(json));
        event.setTableId(StandardEventUtils.getTable(json));
        event.setNamespaces(StandardEventUtils.getNamespaces(json));
    }
}
