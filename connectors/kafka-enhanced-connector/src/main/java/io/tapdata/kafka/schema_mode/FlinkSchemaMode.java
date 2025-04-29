package io.tapdata.kafka.schema_mode;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kafka.AbsSchemaMode;
import io.tapdata.kafka.IKafkaService;
import io.tapdata.kafka.constants.KafkaSchemaMode;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2025-04-22 10:03
 **/
public class FlinkSchemaMode extends AbsSchemaMode {
	public FlinkSchemaMode(IKafkaService kafkaService) {
		super(KafkaSchemaMode.FLINK_CDC, kafkaService);
	}

	@Override
	public void discoverSchema(IKafkaService kafkaService, List<String> tables, int tableSize, Consumer<List<TapTable>> consumer) {
		// todo
		throw new UnsupportedOperationException();
	}

	@Override
	public TapEvent toTapEvent(ConsumerRecord<?, ?> consumerRecord) {
		// todo
		throw new UnsupportedOperationException();
	}

	@Override
	public List<ProducerRecord<Object, Object>> fromTapEvent(TapTable table, TapEvent tapEvent) {
		String topic = topic(table, tapEvent);
		List<ProducerRecord<Object, Object>> producerRecords = new ArrayList<>();
		if (tapEvent instanceof TapInsertRecordEvent) {
			Map<String, Object> value = new HashMap<>();
			Map<String, Object> data = ((TapInsertRecordEvent) tapEvent).getAfter();
			value.put("data", data);
			value.put("op", "+I");
			byte[] key = createKafkaKey(data, table);
			producerRecords.add(new ProducerRecord<>(topic, key, value));
		} else if (tapEvent instanceof TapUpdateRecordEvent) {
			Map<String, Object> before = ((TapUpdateRecordEvent) tapEvent).getBefore();
			Map<String, Object> after = ((TapUpdateRecordEvent) tapEvent).getAfter();
			if (null != before && !before.isEmpty()) {
				Map<String, Object> beforeValue = new HashMap<>();
				beforeValue.put("data", before);
				beforeValue.put("op", "-U");
				byte[] beforeKey = createKafkaKey(before, table);
				producerRecords.add(new ProducerRecord<>(topic, beforeKey, beforeValue));
			}
			Map<String, Object> afterValue = new HashMap<>();
			afterValue.put("data", after);
			afterValue.put("op", "+U");
			byte[] afterKey = createKafkaKey(after, table);
			producerRecords.add(new ProducerRecord<>(topic, afterKey, afterValue));
		} else if (tapEvent instanceof TapDeleteRecordEvent) {
			Map<String, Object> before = ((TapDeleteRecordEvent) tapEvent).getBefore();
			Map<String, Object> value = new HashMap<>();
			value.put("data", before);
			value.put("op", "-D");
			byte[] key = createKafkaKey(before, table);
			producerRecords.add(new ProducerRecord<>(topic, key, value));
		}
		return producerRecords;
	}

	@Override
	public void queryByAdvanceFilter(TapAdvanceFilter filter, TapTable table, Consumer<FilterResults> consumer) {
		// todo
		throw new UnsupportedOperationException();
	}
}
