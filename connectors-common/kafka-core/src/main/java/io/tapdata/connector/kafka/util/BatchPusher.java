package io.tapdata.connector.kafka.util;

import io.tapdata.entity.simplify.TapSimplify;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/2 14:59 Create
 */
public class BatchPusher<T> implements AutoCloseable {

    private int maxDelay = 2000;
    private long lastTime;
    private final Consumer<List<T>> submitConsumer;
    private List<T> batchList;

    public BatchPusher(Consumer<List<T>> submitConsumer) {
        this.batchList = TapSimplify.list();
        this.lastTime = System.currentTimeMillis();
        this.submitConsumer = submitConsumer;
    }

    public BatchPusher<T> maxDelay(int maxDelay) {
        this.maxDelay = maxDelay;
        return this;
    }

    public void add(T record) {
        batchList.add(record);
        checkAndSummit();
    }

    public void checkAndSummit() {
        if (System.currentTimeMillis() - lastTime > maxDelay && !batchList.isEmpty()) {
            summit();
        }
    }

    private void summit() {
        submitConsumer.accept(batchList);
        batchList = TapSimplify.list();
        lastTime = System.currentTimeMillis();
    }

    @Override
    public void close() throws Exception {
        if (null != batchList) {
            if (!batchList.isEmpty()) {
                summit();
            }
            batchList = null;
        }
    }
}
