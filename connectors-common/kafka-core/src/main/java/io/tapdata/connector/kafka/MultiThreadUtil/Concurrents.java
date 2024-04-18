package io.tapdata.connector.kafka.MultiThreadUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * 并发处理工具
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/15 11:39 Create
 */
public abstract class Concurrents<T> implements AutoCloseable {

	protected final AtomicBoolean isClosed = new AtomicBoolean(false);
	protected final Map<String, T> instanceMap = new HashMap<>();

	protected abstract String getInstanceId();

	protected abstract T initNewInstance(String instanceId);

	public <V> V process(Function<T, V> fn) {
		if (isClosed.get()) throw new RuntimeException("Already closed");

		String instanceId = getInstanceId();
		T instance = instanceMap.get(instanceId);
		if (null == instance) {
			synchronized (instanceMap) {
				instance = instanceMap.get(instanceId);
				if (null == instance) {
					instance = initNewInstance(instanceId);
					instanceMap.put(instanceId, instance);
				}
			}
		}
		return fn.apply(instance);
	}

	@Override
	public void close() throws Exception {
		if (isClosed.compareAndSet(false, true)) {
			AtomicReference<Exception> error = new AtomicReference<>(null);
			instanceMap.values().forEach(t -> {
				try {
					if (t instanceof AutoCloseable) {
						((AutoCloseable) t).close();
					}
				} catch (Exception e) {
					if (!error.compareAndSet(null, e)) {
						error.get().addSuppressed(e);
					}
				}
			});
			instanceMap.clear();

			if (null != error.get()) throw error.get();
		}
	}


}
