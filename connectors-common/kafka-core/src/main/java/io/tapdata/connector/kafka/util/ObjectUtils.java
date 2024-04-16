package io.tapdata.connector.kafka.util;

import io.tapdata.base.ConnectorBase;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.TapUtils;

import java.util.*;

public class ObjectUtils {
    public static Object covertData(Object apply) {
        if (Objects.isNull(apply)) {
            return null;
        } else if (apply instanceof Map) {
            return InstanceFactory.instance(TapUtils.class).cloneMap((Map<String, Object>) apply);//fromJson(toJson(apply));
        } else if (apply instanceof Collection) {
            try {
                return new ArrayList<>((List<Object>) apply);//ConnectorBase.fromJsonArray(toJson(apply));
            } catch (Exception e) {
                String toString = apply.toString();
                if (toString.matches("\\(([0-9]+)\\)\\[.*]")) {
                    toString = toString.replaceFirst("\\(([0-9]+)\\)", "");
                }
                return ConnectorBase.fromJsonArray(toString);
            }
        } else{
            return apply;
        }
    }
	public static Map<String, Object> getBefore(TapRecordEvent tapEvent) {
		if (tapEvent instanceof TapUpdateRecordEvent) {
			return ((TapUpdateRecordEvent) tapEvent).getBefore();
		} else if (tapEvent instanceof TapDeleteRecordEvent) {
			return ((TapDeleteRecordEvent) tapEvent).getBefore();
		}
		return null;
	}
	public static Map<String, Object> getAfter(TapRecordEvent tapEvent) {
		if (tapEvent instanceof TapInsertRecordEvent) {
			return ((TapInsertRecordEvent) tapEvent).getAfter();
		} else if (tapEvent instanceof TapUpdateRecordEvent) {
			return ((TapUpdateRecordEvent) tapEvent).getAfter();
		}
		return null;
	}
}
