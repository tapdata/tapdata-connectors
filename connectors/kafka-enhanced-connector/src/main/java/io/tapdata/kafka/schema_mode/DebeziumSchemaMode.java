package io.tapdata.kafka.schema_mode;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.tapdata.connector.error.KafkaErrorCodes;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.exception.TapCodeException;
import io.tapdata.kafka.AbsSchemaMode;
import io.tapdata.kafka.IKafkaService;
import io.tapdata.kafka.constants.KafkaSchemaMode;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
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
public class DebeziumSchemaMode extends AbsSchemaMode {

	private static final Map<String, DebeziumType> debeziumTypeMapper = new HashMap<String, DebeziumType>() {{
		put(TapInsertRecordEvent.class.getName(), DebeziumType.c);
		put(TapUpdateRecordEvent.class.getName(), DebeziumType.u);
		put(TapDeleteRecordEvent.class.getName(), DebeziumType.d);
	}};
	private static final Cache<String, Map<String, String>> dbTypeCache = Caffeine.newBuilder()
			.maximumSize(10L)
			.expireAfterWrite(1L, TimeUnit.HOURS)
			.build();

	public DebeziumSchemaMode(IKafkaService kafkaService) {
		super(KafkaSchemaMode.DEBEZIUM, kafkaService);
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
		byte[] key;
		Map<String, Object> value = new HashMap<>();
		Map<String, Object> source = new HashMap<>();
		processIfStringNotBlank(tapEvent.getPdkId(), pdkId -> source.put("connector", pdkId));
		source.put("ts_ms", ts);
		processIfStringNotBlank(tapEvent.getDatabase(), database -> source.put("db", database));
		processIfStringNotBlank(tapEvent.getSchema(), schema -> source.put("schema", schema));
		processIfStringNotBlank(tapEvent.getInfo("mysql_name"), name -> source.put("name", name));
		processIfStringNotBlank(tapEvent.getInfo("mysql_server_id"), serverId -> source.put("server_id", Integer.parseInt(serverId)));
		processIfStringNotBlank(tapEvent.getInfo("mysql_file"), file -> source.put("file", file));
		processIfStringNotBlank(tapEvent.getInfo("mysql_pos"), pos -> source.put("pos", Integer.parseInt(pos)));
		processIfStringNotBlank(tapEvent.getInfo("mysql_row"), row -> source.put("row", Integer.parseInt(row)));
		Object syncStage = tapEvent.getInfo("SYNC_STAGE");
		if (syncStage instanceof String && ((String) syncStage).equalsIgnoreCase("initial_sync")) {
			source.put("snapshot", "true");
		} else {
			source.put("snapshot", "false");
		}
		source.put("table", table.getId());
		value.put("source", source);
		value.put("op", op(tapEvent).name());
		value.put("ts_ms", System.currentTimeMillis());
		if (tapEvent instanceof TapRecordEvent) {
			Map<String, Object> before = before(tapEvent);
			Map<String, Object> after = after(tapEvent);
			Optional.ofNullable(before).ifPresent(data -> value.put("before", data));
			Optional.ofNullable(after).ifPresent(data -> value.put("after", data));
			key = createKafkaKey((null != after && !after.isEmpty()) ? after : before, table);
		} else {
			throw new TapCodeException(KafkaErrorCodes.DEBEZIUM_NOT_SUPPORT_EVENT).dynamicDescriptionParameters(tapEvent.getClass().getSimpleName());
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

	private Map<String, Object> after(TapEvent tapEvent) {
		if (tapEvent instanceof TapInsertRecordEvent) {
			return ((TapInsertRecordEvent) tapEvent).getAfter();
		} else if (tapEvent instanceof TapUpdateRecordEvent) {
			return ((TapUpdateRecordEvent) tapEvent).getAfter();
		}
		return null;
	}

	private Map<String, Object> before(TapEvent tapEvent) {
		if (tapEvent instanceof TapUpdateRecordEvent) {
			return ((TapUpdateRecordEvent) tapEvent).getBefore();
		} else if (tapEvent instanceof TapDeleteRecordEvent) {
			return ((TapDeleteRecordEvent) tapEvent).getBefore();
		}
		return null;
	}

	private DebeziumType op(TapEvent tapEvent) {
		return debeziumTypeMapper.get(tapEvent.getClass().getName());
	}

	private enum DebeziumType {
		c, u, d
	}
}
