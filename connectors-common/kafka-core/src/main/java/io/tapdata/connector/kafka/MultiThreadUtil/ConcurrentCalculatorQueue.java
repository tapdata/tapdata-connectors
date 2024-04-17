package io.tapdata.connector.kafka.MultiThreadUtil;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ConcurrentCalculatorQueue<P, V> implements AutoCloseable {
	private final AtomicBoolean isClose = new AtomicBoolean(false);
	protected final ExecutorService executorService;
	protected final ArrayBlockingQueue<CompletableFuture<V>> futureQueue;
//	protected final Function<P, V> performComputation;

	protected ConcurrentCalculatorQueue(int threadSize, int queueSize) {
		this.futureQueue = new ArrayBlockingQueue<>(queueSize);
		this.executorService = Executors.newFixedThreadPool(threadSize + 1, this::newThread);
		this.executorService.submit(() -> {
			try {
				while (isRunning()) {
					CompletableFuture<V> future = futureQueue.poll(10, TimeUnit.MILLISECONDS);
					distributingFuture(future);
				}
			} catch (ExecutionException e) {
				handleError(e);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
	}
	protected void distributingFuture(CompletableFuture<V> future) throws ExecutionException, InterruptedException {
		if (null != future) {
			distributingData(future.get());
		}
	}
	protected abstract void distributingData(V data);
	protected Thread newThread(Runnable runnable) {
		Thread thread = new Thread(runnable);
		thread.setDaemon(true);
		return thread;
	}
	public void multiCalc(P val) throws InterruptedException {
		CompletableFuture<V> future = CompletableFuture.supplyAsync(() -> performComputation(val), executorService);
		do {
			if (futureQueue.offer(future, 200, TimeUnit.MILLISECONDS)) break;
		} while (isRunning());
	}
	protected abstract V performComputation(P data);

	protected boolean isRunning() {
		return !Thread.currentThread().isInterrupted();
	}
	protected abstract void handleError(Exception e);
	@Override
	public void close() throws Exception {
		if (isClose.compareAndSet(false, true)) {
			executorService.shutdown();
		}
	}
}
