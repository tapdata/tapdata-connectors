package io.tapdata.connector.kafka.MultiThreadUtil;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.schema.TapTable;

import java.util.Map;

public class WriteEventConvertDto {
	private TapTable tapTable;
	TapEvent tapEvent;
	Map<String,Object> jsConvertResultMap;
	private byte[] kafkaMessageKey;


	public byte[] getKafkaMessageKey() {
		return kafkaMessageKey;
	}

	public void setKafkaMessageKey(byte[] kafkaMessageKey) {
		this.kafkaMessageKey = kafkaMessageKey;
	}

	public Map<String, Object> getJsConvertResultMap() {
		return jsConvertResultMap;
	}

	public void setJsConvertResultMap(Map<String, Object> jsConvertResultMap) {
		this.jsConvertResultMap = jsConvertResultMap;
	}

	public TapTable getTapTable() {
		return tapTable;
	}

	public void setTapTable(TapTable tapTable) {
		this.tapTable = tapTable;
	}

	public TapEvent getTapEvent() {
		return tapEvent;
	}

	public void setTapEvent(TapEvent tapEvent) {
		this.tapEvent = tapEvent;
	}
}
