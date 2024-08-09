package io.tapdata.connector.kafka.data;

import io.tapdata.entity.event.TapEvent;

import java.io.Serializable;

/**
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/26 18:21 Create
 */
public class PerformResults implements Serializable {
	private String topic;
	private KafkaTopicOffset offset;
	private TapEvent tapEvent;

	public PerformResults() {
	}

	public PerformResults(String topic, KafkaTopicOffset offset, TapEvent tapEvent) {
		this.topic = topic;
		this.offset = offset;
		this.tapEvent = tapEvent;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public KafkaTopicOffset getOffset() {
		return offset;
	}

	public void setOffset(KafkaTopicOffset offset) {
		this.offset = offset;
	}

	public TapEvent getTapEvent() {
		return tapEvent;
	}

	public void setTapEvent(TapEvent tapEvent) {
		this.tapEvent = tapEvent;
	}

}
