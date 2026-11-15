package io.tapdata.connector.paimon.service;

import io.tapdata.entity.logger.Log;
import io.tapdata.connector.paimon.util.PaimonBackgroundThreads;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** 类加载器内共享的有界观察队列；日志后端阻塞不能拖住 STOP caller/supervisor。
 * 分发线程懒启动并在空闲 60 秒后退出，允许 ClassLoader 在无活跃任务时被回收。 */
public final class PaimonStopLog {
    private static final AtomicLong DROPPED = new AtomicLong();
    // Executor owns the enqueue/idle-exit handshake; no separate running flag or dispatcher lock.
    private static final ThreadPoolExecutor DISPATCHER = new ThreadPoolExecutor(
            0, 1, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(256),
            new PaimonBackgroundThreads("paimon-stop-log"), (event, executor) -> DROPPED.incrementAndGet());

    private PaimonStopLog() {}

    public static void offer(Log log, String message, Throwable failure) {
        offer(log, message, failure, false, false);
    }

    /** 失败类事件以 ERROR 输出；队列饱和或后端失败计入 droppedCount。 */
    public static void offerError(Log log, String message, Throwable failure) {
        offer(log, message, failure, true, false);
    }

    public static void offerWarn(Log log, String message, Throwable failure) {
        offer(log, message, failure, false, true);
    }

    private static void offer(Log log, String message, Throwable failure, boolean error, boolean warn) {
        if (log != null) {
            DISPATCHER.execute(new Event(log, message, failure, error, warn));
        }
    }
    public static long droppedCount() { return DROPPED.get(); }
    static int queuedCount() { return DISPATCHER.getQueue().size(); }
    private static final class Event implements Runnable {
        final Log log; final String message; final Throwable failure; final boolean error; final boolean warn;
        Event(Log log, String message, Throwable failure, boolean error, boolean warn) {
            this.log = log; this.message = message; this.failure = failure; this.error = error; this.warn = warn;
        }

        @Override
        public void run() {
            try {
                String text = message;
                if (failure != null) {
                    StringWriter stack = new StringWriter();
                    failure.printStackTrace(new PrintWriter(stack));
                    text += " cause=" + stack;
                }
                if (error) {
                    log.error(text);
                } else if (warn) {
                    log.warn(text);
                } else {
                    log.info(text);
                }
            } catch (Throwable ignored) {
                // A failing backend must neither block STOP nor hide failed delivery from the counter.
                DROPPED.incrementAndGet();
            }
        }
    }
}
