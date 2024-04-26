package io.tapdata.connector.kafka.MultiThreadUtil;

import io.tapdata.connector.kafka.data.KafkaStreamOffset;
import io.tapdata.connector.kafka.data.KafkaTopicOffset;
import io.tapdata.connector.kafka.data.PerformResults;
import io.tapdata.connector.kafka.util.CustomParseUtil;
import io.tapdata.connector.kafka.util.ObjectUtils;
import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.exception.StopException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.script.Invocable;
import javax.script.ScriptException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public class CustomParseCalculatorQueue extends ConcurrentCalculatorQueue<ConsumerRecord<byte[], byte[]>, PerformResults>{
	public static final int DEFAULT_SEND_TIMEOUT = 500;
	private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);

	private KafkaStreamOffset streamOffset;
	private List<TapEvent> consumeList = TapSimplify.list();
	private Concurrents<Invocable> customDmlConcurrents;
	protected AtomicBoolean consuming;
	private BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer;
	private int eventBatchSize;
	protected long lastSendTime;

	public CustomParseCalculatorQueue(int threadSize, int queueSize,Concurrents<Invocable> customDmlConcurrents) {
		super(threadSize, queueSize);
		this.customDmlConcurrents = customDmlConcurrents;
		this.lastSendTime = System.currentTimeMillis() + 500;
	}

	public int getEventBatchSize() {
		return eventBatchSize;
	}

	public void setEventBatchSize(int eventBatchSize) {
		this.eventBatchSize = eventBatchSize;
	}

	public AtomicBoolean getConsuming() {
		return consuming;
	}
	public void setConsuming(AtomicBoolean consuming){
		this.consuming=consuming;
	}

	public BiConsumer<List<TapEvent>, Object> getEventsOffsetConsumer() {
		return eventsOffsetConsumer;
	}

	public void setEventsOffsetConsumer(BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) {
		this.eventsOffsetConsumer = eventsOffsetConsumer;
	}

	@Override
	protected void distributingFuture(CompletableFuture<PerformResults> future) throws ExecutionException, InterruptedException {
		super.distributingFuture(future);
		distributingDataDelay();
	}

	private void distributingDataDelay() {
		if (CollectionUtils.isNotEmpty(consumeList) && (System.currentTimeMillis() > lastSendTime || consumeList.size() >= eventBatchSize)) {
			eventsOffsetConsumer.accept(consumeList, streamOffset.clone());
			consumeList = TapSimplify.list();
			this.lastSendTime = System.currentTimeMillis() + DEFAULT_SEND_TIMEOUT;
		}
	}

	@Override
	protected void distributingData(PerformResults data) {
		if (null != data) {
			streamOffset.addTopicOffset(data.getTopic(), data.getOffset());
			consumeList.add(data.getTapEvent());
		}
	}

	public void setStreamOffset(KafkaStreamOffset streamOffset) {
		this.streamOffset = streamOffset;
	}

	public void setConsumeList(List<TapEvent> consumeList) {
		this.consumeList = consumeList;
	}

	public Concurrents<Invocable> getCustomDmlConcurrents() {
		return customDmlConcurrents;
	}

	public void setCustomDmlConcurrents(Concurrents<Invocable> customDmlConcurrents) {
		this.customDmlConcurrents = customDmlConcurrents;
	}

	@Override
	protected PerformResults performComputation(ConsumerRecord<byte[], byte[]> data) {
		return customDmlConcurrents.process((scriptEngine) -> makeCustomMessageUseScriptEngine(scriptEngine, data));
	}

	@Override
	protected void handleError(Exception e) {
		getException().compareAndSet(null, e);
	}

	private PerformResults makeCustomMessageUseScriptEngine(Invocable customScriptEngine, ConsumerRecord<byte[], byte[]> consumerRecord) {
		Map<String, Object> messageBody = jsonParser.fromJsonBytes(consumerRecord.value(), Map.class);
		Object data = ObjectUtils.covertData(executeScript(customScriptEngine, "process", messageBody));
		if (null == data) return null;

		// kafka offset 表示下一个事件的偏移量，所以需要 + 1
		KafkaTopicOffset offset = new KafkaTopicOffset(consumerRecord.partition(), consumerRecord.offset() + 1, consumerRecord.timestamp());
		TapBaseEvent tapBaseEvent = CustomParseUtil.applyCustomParse((Map<String, Object>) data);
		tapBaseEvent.setTableId(consumerRecord.topic());
		return new PerformResults(consumerRecord.topic(), offset, tapBaseEvent);
	}

	public static Object executeScript(Invocable scriptEngine, String function, Object... params) {
		if (scriptEngine != null) {
			Invocable invocable = (Invocable) scriptEngine;
			try {
				return invocable.invokeFunction(function, params);
			} catch (StopException e) {
//                TapLogger.info(TAG, "Get data and stop script.");
				throw new RuntimeException(e);
			} catch (ScriptException | NoSuchMethodException | RuntimeException e) {
//                TapLogger.error(TAG, "Run script error, message: {}", e.getMessage(), e);
				throw new RuntimeException(e);
			}
		}
		return null;
	}
}
