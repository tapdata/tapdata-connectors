package io.tapdata.kafka.constants;

import io.tapdata.entity.event.dml.TapRecordEvent;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author samuel
 * @Description
 * @create 2025-04-21 17:12
 **/
public class ProducerRecordWrapper {
	private final TapRecordEvent recordEvent;
	private final ProducerRecord<Object, Object> producerRecord;

	public ProducerRecordWrapper(TapRecordEvent recordEvent, ProducerRecord<Object, Object> producerRecord) {
		this.recordEvent = recordEvent;
		this.producerRecord = producerRecord;
	}

	public TapRecordEvent getRecordEvent() {
		return recordEvent;
	}

	public ProducerRecord<Object, Object> getProducerRecord() {
		return producerRecord;
	}
}
