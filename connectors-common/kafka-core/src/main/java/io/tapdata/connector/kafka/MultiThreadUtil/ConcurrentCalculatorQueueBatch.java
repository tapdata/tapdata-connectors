package io.tapdata.connector.kafka.MultiThreadUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public abstract class ConcurrentCalculatorQueueBatch<P, V> extends ConcurrentCalculatorQueue<P, V>{
	protected final int batchSize;
	protected final int maxDelay;
	protected long lastSendTime;
	protected List<V> results = new ArrayList<>();
	protected ConcurrentCalculatorQueueBatch(int threadSize, int queueSize, int batchSize, int maxDelay) {
		super(threadSize, queueSize);
		this.batchSize = batchSize;
		this.maxDelay = maxDelay;
		this.lastSendTime = System.currentTimeMillis() + this.maxDelay;

	}
	@Override
	protected void distributingData(V data) {
		results.add(data);
	}

	@Override
	protected void handleError(Exception e) {

	}
	protected void pushBatch() {
		if (results.size() >= batchSize || System.currentTimeMillis() > lastSendTime) {
			sendBatch(results);
			results = new ArrayList<>();
			lastSendTime = System.currentTimeMillis() + maxDelay;
		}
	}
	protected abstract void sendBatch(List<V> dataList);
}
