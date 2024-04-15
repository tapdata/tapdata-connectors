package io.tapdata.connector.kafka.MultiThreadUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * 并发计算
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/15 15:31 Create
 */
public abstract class ConcurrentCalculator<T,V> implements AutoCloseable {

	private final ExecutorService executor;

	protected ConcurrentCalculator(int concurrentSize) {
		executor = Executors.newFixedThreadPool(concurrentSize);
	}

	public List<V> calc(List<T> dataList) throws InterruptedException, ExecutionException {
		List<CompletableFuture<V>> futures = new ArrayList<>();
		for (T data : dataList) {
			CompletableFuture<V> future = CompletableFuture.supplyAsync(() -> performComputation(data), executor);
			futures.add(future);
		}

		List<V> results = new ArrayList<>();
		for (CompletableFuture<V> future : futures) {
			results.add(future.get());
		}
		return results;
	}

	public void calc(List<T> dataList, Consumer<V> callback) throws InterruptedException, ExecutionException {
		List<CompletableFuture<V>> futures = new ArrayList<>();
		for (T data : dataList) {
			CompletableFuture<V> future = CompletableFuture.supplyAsync(() -> performComputation(data), executor);
			futures.add(future);
		}

		for (CompletableFuture<V> future : futures) {
			callback.accept(future.get());
		}
	}

	protected abstract V performComputation(T data);

	@Override
	public void close() throws Exception {
		executor.shutdown();
	}
}
