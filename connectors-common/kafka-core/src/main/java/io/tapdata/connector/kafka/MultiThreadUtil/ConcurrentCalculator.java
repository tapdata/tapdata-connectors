package io.tapdata.connector.kafka.MultiThreadUtil;

import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * 并发计算
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/15 15:31 Create
 */
public abstract class ConcurrentCalculator<T,V> implements AutoCloseable {
	private final AtomicBoolean isClose = new AtomicBoolean(false);
	private final ExecutorService executorService;

	protected ConcurrentCalculator(int concurrentSize) {
		executorService = Executors.newFixedThreadPool(concurrentSize);
    }

	public List<V> calc(List<T> dataList) throws InterruptedException, ExecutionException {
		List<CompletableFuture<V>> futures = new ArrayList<>();
		for (T data : dataList) {
			CompletableFuture<V> future = CompletableFuture.supplyAsync(() -> performComputation(data), executorService);
			futures.add(future);
		}

		List<V> results = new ArrayList<>();
		for (CompletableFuture<V> future : futures) {
			results.add(future.get());
		}
		return results;
	}
	public List<V> calcList(List<T> dataList) throws InterruptedException, ExecutionException {
		List<CompletableFuture<List<V>>> futures = new ArrayList<>();
		CopyOnWriteArraySet<List<T>> dataLists = new CopyOnWriteArraySet<>(DbKit.splitToPieces(dataList, 8));
		List<T> subList;
		while ((subList = getOutTableList(dataLists)) != null) {
			List<T> finalSubList = subList;
			CompletableFuture<List<V>> future = CompletableFuture.supplyAsync(() -> performComputation(finalSubList),executorService);
			futures.add(future);
		}
		List<V> results = new ArrayList<>();
		for (CompletableFuture<List<V>> future : futures) {
			results.addAll(future.get());
		}
		return results;
	}
	private synchronized List<T> getOutTableList(CopyOnWriteArraySet<List<T>> dataLists) {
		if (EmptyKit.isNotEmpty(dataLists)) {
			List<T> list = dataLists.stream().findFirst().orElseGet(ArrayList::new);
			dataLists.remove(list);
			return list;
		}
		return null;
	}
	public void calc(List<T> dataList, Consumer<V> callback) throws InterruptedException, ExecutionException {
		List<CompletableFuture<V>> futures = new ArrayList<>();
		for (T data : dataList) {
			CompletableFuture<V> future = CompletableFuture.supplyAsync(() -> performComputation(data), executorService);
			futures.add(future);
		}

		for (CompletableFuture<V> future : futures) {
			callback.accept(future.get());
		}
	}

	protected abstract V performComputation(T data);

	protected abstract List<V> performComputation(List<T> data);

	protected boolean isRunning() {
		return !Thread.currentThread().isInterrupted();
	}
	@Override
	public void close() throws Exception {
		executorService.shutdown();
	}
}
