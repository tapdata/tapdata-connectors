package io.tapdata.connector.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * 并发处理工具
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/7 15:40 Create
 */
public interface ConcurrentUtils {
    Logger LOGGER = LoggerFactory.getLogger(ConcurrentUtils.class);

    static <T> void runWithQueue(ExecutorService executorService, String name, Queue<T> queue, int limit, BooleanSupplier stopping, Runner<T> consumer) throws Exception {
        AtomicReference<Exception> exception = new AtomicReference<>();
        int concurrentSize = Math.min(limit, queue.size());
        List<CompletableFuture<Void>> futures = new ArrayList<>(concurrentSize);
        for (int i = 0; i < concurrentSize; i++) {
            int finalIndex = i;
            futures.add(CompletableFuture.runAsync(() -> {
                String threadName = String.format("%s-%d", name, finalIndex);
                Thread.currentThread().setName(threadName);
                LOGGER.debug("Starting of queue {}", threadName);
                try {
                    T val;
                    while (!stopping.getAsBoolean() && null == exception.get()) {
                        val = queue.poll();
                        if (null == val) break;

                        consumer.run(val);
                    }
                } catch (Exception e) {
                    exception.compareAndSet(null, e);
                } finally {
                    LOGGER.debug("Completed of queue {}", threadName);
                }
            }, executorService));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{})).join();
        if (exception.get() != null) {
            throw exception.get();
        }
    }

    static <T> void runWithQueue(ExecutorService executorService, String name, Queue<T> queue, int limit, Runner<T> consumer) throws Exception {
        runWithQueue(executorService, name, queue, limit, Thread::interrupted, consumer);
    }

    static <T> void runWithList(ExecutorService executorService, String name, List<T> items, IndexRunner<T> consumer) throws Exception {
        AtomicReference<Exception> exception = new AtomicReference<>();
        int latchSize = items.size();
        List<CompletableFuture<Void>> futures = new ArrayList<>(latchSize);
        for (int i = 0; i < latchSize; i++) {
            final int finalIndex = i;
            final T val = items.get(i);

            futures.add(CompletableFuture.runAsync(() -> {
                String threadName = String.format("%s-%d", name, finalIndex);
                Thread.currentThread().setName(threadName);
                LOGGER.debug("Starting of list {}", threadName);
                try {
                    consumer.run(exception, finalIndex, val);
                } catch (Exception e) {
                    if (!exception.compareAndSet(null, e)) {
                        exception.get().addSuppressed(e);
                    }
                } finally {
                    LOGGER.debug("Completed of list {}", threadName);
                }
            }, executorService));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[]{})).join();
        if (exception.get() != null) {
            throw exception.get();
        }
    }


    interface Runner<T> {
        void run(T v) throws Exception;
    }

    interface IndexRunner<T> {
        void run(AtomicReference<Exception> ex, int index, T v) throws Exception;
    }
}
