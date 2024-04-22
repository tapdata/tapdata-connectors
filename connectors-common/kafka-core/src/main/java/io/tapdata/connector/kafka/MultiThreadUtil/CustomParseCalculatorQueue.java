package io.tapdata.connector.kafka.MultiThreadUtil;

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

public class CustomParseCalculatorQueue extends ConcurrentCalculatorQueue<ConsumerRecord<byte[], byte[]>, TapEvent>{
	private List<TapEvent> consumeList = TapSimplify.list();
	private Concurrents<Invocable> customDmlConcurrents;
	private static final JsonParser jsonParser = InstanceFactory.instance(JsonParser.class);
	protected AtomicBoolean consuming;
	BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer;
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
	protected void distributingFuture(CompletableFuture<TapEvent> future) throws ExecutionException, InterruptedException {
		super.distributingFuture(future);
		distributingDataDelay();
	}

	private void distributingDataDelay() {
		if (CollectionUtils.isNotEmpty(consumeList) && (System.currentTimeMillis() > lastSendTime || consumeList.size() >= eventBatchSize)) {
			distributingDataBatch(consumeList);
			consumeList = TapSimplify.list();
			this.lastSendTime = System.currentTimeMillis() + 500;
		}
	}

	@Override
	protected void distributingData(TapEvent data) {
		if (null != data) {
			consumeList.add(data);
		}
//		if (consumeList.size() >= eventBatchSize) {
//			List<TapEvent> engineConsumeList = consumeList;
//			eventsOffsetConsumer.accept(engineConsumeList, TapSimplify.list());
//			consumeList = TapSimplify.list();
//		}
	}

	protected void distributingDataBatch(List<TapEvent> data) {
		eventsOffsetConsumer.accept(data, TapSimplify.list());
//		if (null != data) {
//			consumeList.add(data);
//		}
//		if (consumeList.size() >= eventBatchSize) {
//			List<TapEvent> engineConsumeList = consumeList;
//			eventsOffsetConsumer.accept(engineConsumeList, TapSimplify.list());
//			consumeList = TapSimplify.list();
//		}
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
	protected TapEvent performComputation(ConsumerRecord<byte[], byte[]> data) {
		return customDmlConcurrents.process((scriptEngine) -> makeCustomMessageUseScriptEngine(scriptEngine, data));
	}

	@Override
	protected void handleError(Exception e) {
		if(getHasException().compareAndSet(false, true)){
			getException().set(e);
		}
	}

	private TapEvent makeCustomMessageUseScriptEngine(Invocable customScriptEngine, ConsumerRecord<byte[], byte[]> consumerRecord) {
		Map<String, Object> messageBody = jsonParser.fromJsonBytes(consumerRecord.value(), Map.class);
		Object data = ObjectUtils.covertData(executeScript(customScriptEngine, "process", messageBody));
		if (null == data) return null;
		TapBaseEvent tapBaseEvent = CustomParseUtil.applyCustomParse((Map<String, Object>) data);
		tapBaseEvent.setTableId(consumerRecord.topic());
		return tapBaseEvent;
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
