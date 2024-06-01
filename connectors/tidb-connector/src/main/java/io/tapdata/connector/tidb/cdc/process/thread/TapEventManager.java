package io.tapdata.connector.tidb.cdc.process.thread;

import io.tapdata.connector.tidb.cdc.process.TiData;
import io.tapdata.connector.tidb.cdc.process.ddl.entity.DDLObject;
import io.tapdata.connector.tidb.cdc.process.dml.entity.DMLObject;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

class TapEventManager implements Activity {
    LinkedBlockingQueue<TiData> data;
    ScheduledFuture<?> scheduledFuture;
    final ProcessHandler handler;
    final AtomicReference<Throwable> throwableCollector;
    final StreamReadConsumer consumer;
    final Log log;
    final Object emitLock;

    protected TapEventManager(ProcessHandler handler, int maxQueueSize, StreamReadConsumer consumer) {
        this.handler = handler;
        if (maxQueueSize < 1 || maxQueueSize > 5000) maxQueueSize = 500;
        this.data = new LinkedBlockingQueue<>(maxQueueSize);
        this.throwableCollector = handler.processInfo.throwableCollector;
        this.consumer = consumer;
        this.log = handler.processInfo.nodeContext.getLog();
        this.emitLock = new Object();
    }

    public void emit(Map<String, List<? extends TiData>> dataMap) {
        synchronized (this.emitLock) {
            dataMap.forEach((table, dataList) -> data.addAll(dataList));
        }
    }

    public void emit(TiData tiData) {
        synchronized (this.emitLock) {
            data.add(tiData);
        }
    }

    protected TapEvent eventGeneric(TiData tiData) {

        return null;
    }

    @Override
    public void init() {

    }

    @Override
    public void doActivity() {
        cancelSchedule(this.scheduledFuture, log);
        this.scheduledFuture = this.handler.getScheduledExecutorService().scheduleWithFixedDelay(() -> {
            try {
                while (!data.isEmpty()) {
                    TiData poll = data.poll();
                    if (poll instanceof DMLObject) {
                        handleDML((DMLObject) poll);
                    } else if (poll instanceof DDLObject) {
                        handleDDL((DDLObject) poll);
                    } else {
                        //@todo
                    }
                }
            } catch (Throwable t) {
                synchronized (throwableCollector) {
                    throwableCollector.set(t);
                }
            }
        }, 2, 1, TimeUnit.SECONDS);
    }

    protected void handleDDL(DDLObject ddlObject) {
        System.out.println("a ddl: " + TapSimplify.toJson(ddlObject));
    }

    protected void handleDML(DMLObject dmlObject) {
        System.out.println("a dml: " + TapSimplify.toJson(dmlObject));
    }

    @Override
    public void close() throws Exception {
        cancelSchedule(this.scheduledFuture, log);
        this.scheduledFuture = null;
    }
}
