package io.tapdata.connector.kafka.MultiThreadUtil;

import io.tapdata.common.constant.MqOp;
import io.tapdata.connector.kafka.util.ObjectUtils;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.ddl.TapDDLEvent;
import io.tapdata.entity.event.ddl.table.TapFieldBaseEvent;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.kit.EmptyKit;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import javax.script.Invocable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import static io.tapdata.common.constant.MqOp.*;
import static io.tapdata.connector.kafka.KafkaService.executeScript;
import static io.tapdata.connector.kafka.KafkaService.getDDLRecordData;

public class CustomWriteCalculatorQueue<P,V> extends ConcurrentCalculatorQueue<WriteEventConvertDto, WriteEventConvertDto>{
	private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
	private final Concurrents<Invocable> customDmlConcurrents;
	private final Concurrents<Invocable> customDdlConcurrents;
	private ProduceCustomDMLRecordInfo produceCustomDmlRecordInfo;
	private ProduceCustomDDLRecordInfo produceCustomDdlRecordInfo;

	public CustomWriteCalculatorQueue(int threadSize, int queueSize, Concurrents<Invocable> customDmlConcurrents, Concurrents<Invocable> customDdlConcurrents) {
		super(threadSize, queueSize);
		this.customDmlConcurrents=customDmlConcurrents;
        this.customDdlConcurrents = customDdlConcurrents;
    }

	public ProduceCustomDMLRecordInfo getProduceCustomDmlRecordInfo() {
		return produceCustomDmlRecordInfo;
	}

	public void setProduceCustomDmlRecordInfo(ProduceCustomDMLRecordInfo produceCustomDmlRecordInfo) {
		this.produceCustomDmlRecordInfo = produceCustomDmlRecordInfo;
	}

	public ProduceCustomDDLRecordInfo getProduceCustomDdlRecordInfo() {
		return produceCustomDdlRecordInfo;
	}

	public void setProduceCustomDdlRecordInfo(ProduceCustomDDLRecordInfo produceCustomDdlRecordInfo) {
		this.produceCustomDdlRecordInfo = produceCustomDdlRecordInfo;
	}

	@Override
	protected void distributingData(WriteEventConvertDto writeEventConvertDto) {
		TapEvent event = writeEventConvertDto.getTapEvent();
		if(event instanceof TapRecordEvent){
			TapRecordEvent tapRecordEvent=(TapRecordEvent) event;
			if (null == writeEventConvertDto.getJsConvertResultMap()) {
				produceCustomDmlRecordInfo.getCountDownLatch().countDown();
				return;
			}
			Map<String, Object> jsConvertResultMap = writeEventConvertDto.getJsConvertResultMap();
			byte[] body = {};
			RecordHeaders recordHeaders = new RecordHeaders();
			if (null == jsConvertResultMap.get("data")) {
				throw new RuntimeException("data cannot be null");
			} else {
				Object jsConvertData = jsConvertResultMap.get("data");
				if (jsConvertData instanceof Map) {
					Map<String, Map<String, Object>> jsConvertDataMap = (Map<String, Map<String, Object>>) jsConvertResultMap.get("data");
					removeIfEmptyInMap(jsConvertDataMap, "before");
					removeIfEmptyInMap(jsConvertDataMap, "after");
					body = jsonParser.toJsonBytes(jsConvertDataMap, JsonParser.ToJsonFeature.WriteMapNullValue);
				} else {
					body = jsConvertData.toString().getBytes();
				}
			}
			String mqOp = MapUtils.getString(jsConvertResultMap, "op");
			if (jsConvertResultMap.containsKey("header")) {
				Object obj = jsConvertResultMap.get("header");
				if (obj instanceof Map) {
					Map<String, Object> head = (Map<String, Object>) jsConvertResultMap.get("header");
					for (String s : head.keySet()) {
						recordHeaders.add(s, head.get(s).toString().getBytes());
					}
				} else {
					throw new RuntimeException("header must be a collection type");
				}
			} else {
				recordHeaders.add("mqOp", mqOp.toString().getBytes());
			}
			MqOp finalMqOp = MqOp.fromValue(mqOp);

			Callback callback = (metadata, exception) -> {
				try {
					if (EmptyKit.isNotNull(exception)) {
						this.produceCustomDmlRecordInfo.getListResult().addError(tapRecordEvent, exception);
						return;
					}
					switch (finalMqOp) {
						case INSERT:
							produceCustomDmlRecordInfo.getInsert().incrementAndGet();
							break;
						case UPDATE:
							produceCustomDmlRecordInfo.getUpdate().incrementAndGet();
							break;
						case DELETE:
							produceCustomDmlRecordInfo.getDelete().incrementAndGet();
							break;
						default:
							throw new RuntimeException("mqOp must be insert,update,delete");
					}
				} finally {
					produceCustomDmlRecordInfo.getCountDownLatch().countDown();
				}
			};
			ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(writeEventConvertDto.getTapTable().getId(),
				null, null, writeEventConvertDto.getKafkaMessageKey(), body,
				recordHeaders);
			produceCustomDmlRecordInfo.getKafkaProducer().send(producerRecord, callback);
		} else if (event instanceof TapDDLEvent) {
			AtomicReference<Throwable> reference = new AtomicReference<>();
			TapDDLEvent tapDDLEvent = (TapDDLEvent) event;
			if(null == writeEventConvertDto.getJsConvertResultMap()){
				produceCustomDdlRecordInfo.getCountDownLatch().countDown();
				return;
			}
			Map<String, Object> jsConvertResultMap = writeEventConvertDto.getJsConvertResultMap();
			byte[] body = jsonParser.toJsonBytes(jsConvertResultMap, JsonParser.ToJsonFeature.WriteMapNullValue);
			ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(tapDDLEvent.getTableId(),
				null, tapDDLEvent.getTime(), null, body,
				new RecordHeaders());
			Callback callback = (metadata, exception) -> {
				try{
					if (EmptyKit.isNotNull(exception)) {
						RuntimeException runtimeException = new RuntimeException(reference.get());
						handleError(runtimeException);
						throw runtimeException;
					}
				}finally {
					produceCustomDdlRecordInfo.getCountDownLatch().countDown();
				}
			};
			produceCustomDdlRecordInfo.getKafkaProducer().send(producerRecord, callback);
		}else{
			throw new RuntimeException("not support event");
		}

	}

	@Override
	protected WriteEventConvertDto performComputation(WriteEventConvertDto writeEventConvertDto) {
		TapEvent tapEvent = writeEventConvertDto.getTapEvent();
		if (tapEvent instanceof TapRecordEvent) {
			return customDmlConcurrents.process(scriptEngine -> {
				Collection<String> primaryKeys = writeEventConvertDto.getTapTable().primaryKeys(true);
				TapRecordEvent event = (TapRecordEvent) writeEventConvertDto.getTapEvent();
				Map<String, Object> jsProcessParam = new HashMap<>();
				Map<String, Map<String, Object>> allData = new HashMap();
				MqOp mqOp = INSERT;
				Map<String, Object> eventInfo = new HashMap<>();
				String xid = MapUtils.getString(event.getInfo(), "XID");
				String rowId = MapUtils.getString(event.getInfo(), "rowId");
				if (StringUtils.isNotEmpty(xid)) {
					eventInfo.put("XID", xid);
				}
				if (StringUtils.isNotEmpty(rowId)) {
					eventInfo.put("rowId", rowId);
				}
				eventInfo.put("tableId", event.getTableId());
				eventInfo.put("referenceTime", event.getReferenceTime());
				jsProcessParam.put("eventInfo", eventInfo);
				Map<String, Object> data;
				if (event instanceof TapInsertRecordEvent) {
					data = ((TapInsertRecordEvent) event).getAfter();
					allData.put("before", new HashMap<String, Object>());
					allData.put("after", data);
				} else if (event instanceof TapUpdateRecordEvent) {
					data = ((TapUpdateRecordEvent) event).getAfter();
					Map<String, Object> before = ((TapUpdateRecordEvent) event).getBefore();
					allData.put("before", null == before ? new HashMap<>() : before);
					allData.put("after", data);
					mqOp = UPDATE;
				} else if (event instanceof TapDeleteRecordEvent) {
					data = ((TapDeleteRecordEvent) event).getBefore();
					allData.put("before", data);
					allData.put("after", new HashMap<String, Object>());
					mqOp = DELETE;
				} else {
					data = new HashMap<>();
				}
				byte[] kafkaMessageKey = getKafkaMessageKey(data, writeEventConvertDto.getTapTable());
				writeEventConvertDto.setKafkaMessageKey(kafkaMessageKey);
				jsProcessParam.put("data", allData);
				String op = mqOp.getOp();
				Map<String, Object> header = new HashMap();
				header.put("mqOp", op);
				jsProcessParam.put("header", header);
				Object jsConvertResult = ObjectUtils.covertData(executeScript(scriptEngine, "process", jsProcessParam, op, primaryKeys));
				if (null != jsConvertResult) {
					Map<String, Object> jsConvertResultMap = (Map<String, Object>) jsConvertResult;
					jsConvertResultMap.put("op", op);
					writeEventConvertDto.setJsConvertResultMap(jsConvertResultMap);
				}
				return writeEventConvertDto;
			});
		} else if (tapEvent instanceof TapDDLEvent) {
			return customDdlConcurrents.process(scriptEngine -> {
				TapDDLEvent ddlEvent = (TapFieldBaseEvent) tapEvent;
				Map<String, Object> ddlRecordData = getDDLRecordData(ddlEvent);
				Object jsRecordData = ObjectUtils.covertData(executeScript(scriptEngine, "process", ddlRecordData));
				writeEventConvertDto.setJsConvertResultMap((Map<String, Object>) jsRecordData);
				return writeEventConvertDto;
			});
		} else {
			throw new RuntimeException("not support other event");
		}
	}

	@Override
	protected void handleError(Exception e) {
		getException().compareAndSet(null, e);
	}

	private byte[] getKafkaMessageKey(Map<String, Object> data, TapTable tapTable) {
		if (EmptyKit.isEmpty(tapTable.primaryKeys(true))) {
			return null;
		} else {
			return jsonParser.toJsonBytes(tapTable.primaryKeys(true).stream().map(key -> String.valueOf(data.get(key))).collect(Collectors.joining("_")));
		}
	}

	private void removeIfEmptyInMap(Map<String, Map<String, Object>> map, String key) {
		if (!map.containsKey(key)) return;
		Map<String, Object> o = map.get(key);
		if (null == o || o.isEmpty()) {
			map.remove(key);
		}
	}
}
