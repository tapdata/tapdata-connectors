package io.tapdata.connector.utils;

import io.tapdata.entity.mapping.TapEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * 批处理异步推送工具
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/8 22:51 Create
 */
public class AsyncBatchPusher<O, T> implements Runnable, AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncBatchPusher.class);

    private int batchSize = 100;
    private long maxDelay = 2000;
    private long lastTime;
    private final AtomicReference<Exception> exception;
    private final String name;
    private final Consumer<List<T>> submitConsumer;
    private final Consumer<O> offsetConsumer;
    private final BlockingQueue<TapEntry<O, T>> batchQueue = new LinkedBlockingQueue<>(10);

    private final BooleanSupplier isRunning;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final AtomicBoolean completed = new AtomicBoolean(false);

    public AsyncBatchPusher(AtomicReference<Exception> exception, String name, Consumer<O> offsetConsumer, Consumer<List<T>> submitConsumer, BooleanSupplier isRunning) {
        this.lastTime = System.currentTimeMillis();
        this.exception = exception;
        this.name = name;
        this.offsetConsumer = offsetConsumer;
        this.submitConsumer = submitConsumer;
        this.isRunning = isRunning;
    }

    public AsyncBatchPusher<O, T> batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public AsyncBatchPusher<O, T> maxDelay(long maxDelay) {
        this.maxDelay = maxDelay;
        return this;
    }

    @Override
    public void run() {
        List<T> batchList = new ArrayList<>(batchSize);
        try {
            Thread.currentThread().setName(name);
            LOGGER.info("Starting async batch pusher for {}", name);
            TapEntry<O, T> v;
            while (isRunning.getAsBoolean()) {
                try {
                    v = batchQueue.poll(maxDelay, TimeUnit.MILLISECONDS);

                    // If stopping and no more data in queue, exit the loop
                    if (stopping.get() && null == v) {
                        break;
                    }

                    // Process the polled data
                    if (null != v) {
                        offsetConsumer.accept(v.getKey());
                        batchList.add(v.getValue());
                    }

                    // Push batch if size threshold or time threshold is reached
                    if (batchList.size() >= batchSize || (System.currentTimeMillis() - lastTime > maxDelay && !batchList.isEmpty())) {
                        batchList = push(batchList);
                    }
                } catch (Exception e) {
                    if (!exception.compareAndSet(null, e)) {
                        exception.get().addSuppressed(e);
                    }
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        } finally {
            // Flush remaining data in batchList before completing
            if (!batchList.isEmpty()) {
                try {
                    LOGGER.info("Flushing {} remaining records for {}", batchList.size(), name);
                    push(batchList);
                } catch (Exception e) {
                    LOGGER.error("Error flushing remaining data for {}", name, e);
                    if (!exception.compareAndSet(null, e)) {
                        exception.get().addSuppressed(e);
                    }
                }
            }
            completed.set(true);
            LOGGER.info("Completed async batch pusher for {}", name);
        }
    }

    public void add(O offset, T record) {
        try {
            while (!offer(new TapEntry<>(offset, record))) {
                if (!isRunning.getAsBoolean()) {
                    throw new InterruptedException("not running");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean offer(TapEntry<O, T> record) throws InterruptedException {
        return batchQueue.offer(record, 50, TimeUnit.MILLISECONDS);
    }

    private List<T> push(List<T> batchList) {
        submitConsumer.accept(batchList);
        batchList = new ArrayList<>(batchSize);
        lastTime = System.currentTimeMillis();
        return batchList;
    }

    @Override
    public void close() throws Exception {
        stopping.set(true);
        LOGGER.info("Stopping async batch pusher for {}, queue size: {}", name, batchQueue.size());

        // Wait for the run thread to complete (which will flush remaining data)
        while (!completed.get()) {
            TimeUnit.MILLISECONDS.sleep(50);
        }

        LOGGER.info("Async batch pusher for {} stopped, final queue size: {}", name, batchQueue.size());
    }

    public static <O, T> AsyncBatchPusher<O, T> create(AtomicReference<Exception> exception, String name, Consumer<O> offsetConsumer, Consumer<List<T>> submitConsumer, BooleanSupplier isRunning) {
        return new AsyncBatchPusher<>(exception, name, offsetConsumer, submitConsumer, isRunning);
    }
}
