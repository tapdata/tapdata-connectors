package io.tapdata.connector.kafka.MultiThreadUtil;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * QPS 统计，每秒输出1次
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/15 16:38 Create
 */
public class ShowQps<T> implements AutoCloseable {

	private final long beginTime;
	private final long interval;
	private final BiConsumer<ShowQps<T>, T> callback;
	private long lastPrintTime = 0;
	private final AtomicLong counts = new AtomicLong(0);

	public ShowQps(long interval, BiConsumer<ShowQps<T>, T> callback) {
		this.beginTime = System.currentTimeMillis();
		this.interval = interval;
		this.callback = callback;
	}

	public void process(T param) {
		counts.incrementAndGet();
		if (System.currentTimeMillis() - lastPrintTime > interval) {
			lastPrintTime = System.currentTimeMillis();
			callback.accept(this, param);
		}
	}

	@Override
	public String toString() {
		double qps = 0;
		if (counts.get() > 0) {
			qps = counts.get();
			long duration = System.currentTimeMillis() - beginTime;
			if (duration > 0) {
				qps = counts.get() / (duration / 1000.0);
			}
		}

		return "counts:" + counts.get() + ", qps:" + qps;
	}

	@Override
	public void close() throws Exception {
		callback.accept(this, null);
	}
}
