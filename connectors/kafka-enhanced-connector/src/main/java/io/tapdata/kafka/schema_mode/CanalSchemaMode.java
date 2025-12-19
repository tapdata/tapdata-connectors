package io.tapdata.kafka.schema_mode;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kafka.AbsSchemaMode;
import io.tapdata.kafka.IKafkaService;
import io.tapdata.kafka.constants.KafkaSchemaMode;
import io.tapdata.kafka.utils.KafkaUtils;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2025-04-15 12:24
 **/
public class CanalSchemaMode extends AbsSchemaMode {

	private static final Map<String, CanalType> canalTypeMapper = new HashMap<String, CanalType>() {{
		put(TapInsertRecordEvent.class.getName(), CanalType.INSERT);
		put(TapUpdateRecordEvent.class.getName(), CanalType.UPDATE);
		put(TapDeleteRecordEvent.class.getName(), CanalType.DELETE);
		put(TapDDLEvent.class.getName(), CanalType.QUERY);
	}};
	private static final Cache<String, Map<String, String>> dbTypeCache = Caffeine.newBuilder()
			.maximumSize(10L)
			.expireAfterWrite(1L, TimeUnit.HOURS)
			.build();

	public CanalSchemaMode(IKafkaService kafkaService) {
		super(KafkaSchemaMode.CANAL, kafkaService);
	}

	@Override
	public TapEvent toTapEvent(ConsumerRecord<?, ?> consumerRecord) {
		// todo
		throw new UnsupportedOperationException();
	}

	@Override
	public List<ProducerRecord<Object, Object>> fromTapEvent(TapTable table, TapEvent tapEvent) {
		String topic = topic(table, tapEvent);
		Long ts = tapEvent.getTime();
		byte[] key = null;
		Map<String, Object> value = new HashMap<>();
		value.put("id", 0);
		processIfStringNotBlank(tapEvent.getDatabase(), database -> value.put("database", database));
		processIfStringNotBlank(tapEvent.getSchema(), schema -> value.put("schema", schema));
		value.put("es", ts);
		value.put("ts", ts);
		value.put("type", type(tapEvent).name());
		value.put("table", table.getId());
		if (tapEvent instanceof TapDDLEvent) {
			value.put("isDdl", true);
			value.put("sql", ((TapDDLEvent) tapEvent).getOriginDDL());
			value.put("data", null);
			value.put("old", null);
			String pdkId = tapEvent.getPdkId();
			if (StringUtils.isNotBlank(pdkId) && StringUtils.containsAny(pdkId, "mysql", "mariadb")) {
				value.put("mysqlType", null);
			} else {
				value.put("dbType", null);
			}
			value.put("pkNames", null);
		} else {
			Map<String, Object> old = old(tapEvent);
			Map<String, Object> data = data(tapEvent);
			value.put("isDdl", false);
			value.put("data", data);
			if (null != old && !old.isEmpty()) {
				value.put("old", old);
			}
			Map<String, String> dbType = dbType(table);
			String pdkId = tapEvent.getPdkId();
			if (StringUtils.isNotBlank(pdkId) && StringUtils.containsAny(pdkId, "mysql", "mariadb")) {
				value.put("mysqlType", dbType);
			} else {
				value.put("dbType", dbType);
			}
			value.put("pkNames", pkNames(table));
			key = createKafkaKey(data, table);
		}

		if (null == key) {
			return Arrays.asList(new ProducerRecord<>(topic, value));
		} else {
			return Arrays.asList(new ProducerRecord<>(topic, key, value));
		}
	}

	@Override
	public void queryByAdvanceFilter(TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) {
		// todo
		throw new UnsupportedOperationException();
	}

	private Map<String, Object> data(TapEvent tapEvent) {
		if (tapEvent instanceof TapInsertRecordEvent) {
			return ((TapInsertRecordEvent) tapEvent).getAfter();
		} else if (tapEvent instanceof TapUpdateRecordEvent) {
			return ((TapUpdateRecordEvent) tapEvent).getAfter();
		} else if (tapEvent instanceof TapDeleteRecordEvent) {
			return ((TapDeleteRecordEvent) tapEvent).getBefore();
		}
		return null;
	}

	private Map<String, Object> old(TapEvent tapEvent) {
		if (tapEvent instanceof TapUpdateRecordEvent) {
			return ((TapUpdateRecordEvent) tapEvent).getBefore();
		}
		return null;
	}

	private CanalType type(TapEvent tapEvent) {
		return canalTypeMapper.get(tapEvent.getClass().getName());
	}

	private Map<String, String> dbType(TapTable table) {
		String tableId = table.getId();
		return dbTypeCache.get(tableId, key -> {
			Map<String, String> map = new HashMap<>();
			LinkedHashMap<String, TapField> nameFieldMap = table.getNameFieldMap();
			if (null != nameFieldMap || !nameFieldMap.isEmpty()) {
				nameFieldMap.forEach((fieldName, field) -> map.put(fieldName, field.getDataType()));
			}
			return map;
		});
	}

	private String[] pkNames(TapTable table) {
		Collection<String> pks = table.primaryKeys(true);
		if (null != pks) {
			return pks.toArray(new String[]{});
		} else {
			return null;
		}
	}

	private enum CanalType {
		INSERT, UPDATE, DELETE, QUERY
	}
}
