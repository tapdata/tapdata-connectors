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
    private final String name;
    private final Consumer<List<T>> submitConsumer;
    private final Consumer<O> offsetConsumer;
    private final BlockingQueue<TapEntry<O, T>> batchQueue = new LinkedBlockingQueue<>(10);

    private final BooleanSupplier isRunning;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final AtomicBoolean completed = new AtomicBoolean(false);

    public AsyncBatchPusher(String name, Consumer<O> offsetConsumer, Consumer<List<T>> submitConsumer, BooleanSupplier isRunning) {
        this.lastTime = System.currentTimeMillis();
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
        try {
            Thread.currentThread().setName(name);
            LOGGER.info("Starting async batch pusher for {}", name);
            TapEntry<O, T> v;
            List<T> batchList = new ArrayList<>(batchSize);
            while (isRunning.getAsBoolean()) {
                try {
                    if (stopping.get()) {
                        v = batchQueue.poll(maxDelay, TimeUnit.MILLISECONDS);
                        if (null == v) return;
                    } else {
                        v = batchQueue.poll(maxDelay, TimeUnit.MILLISECONDS);
                        if (null != v) {
                            offsetConsumer.accept(v.getKey());
                            batchList.add(v.getValue());
                        }
                    }

                    if (batchList.size() >= batchSize || (System.currentTimeMillis() - lastTime > maxDelay && !batchList.isEmpty())) {
                        batchList = push(batchList);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } finally {
            completed.set(true);
            LOGGER.info("Completed async batch pusher for {}", name);
        }
    }

    public void add(O offset, T record) {
        try {
            while (!offer(new TapEntry<>(offset, record))) {
                if (!isRunning.getAsBoolean())  {
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
        LOGGER.info("Stopping async batch pusher for {}", name);
        while (!completed.get()) {
            TimeUnit.MILLISECONDS.sleep(50);
        }
    }

    public static <O, T> AsyncBatchPusher<O, T> create(String name, Consumer<O> offsetConsumer, Consumer<List<T>> submitConsumer, BooleanSupplier isRunning) {
        return new AsyncBatchPusher<>(name, offsetConsumer, submitConsumer, isRunning);
    }
}
