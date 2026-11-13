package io.tapdata.connector.paimon.service;

import io.tapdata.entity.logger.Log;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/** 类加载器内共享的有界观察队列；日志后端阻塞不能拖住 STOP caller/supervisor。
 * 分发线程懒启动并在空闲 60 秒后退出，允许 ClassLoader 在无活跃任务时被回收。 */
public final class PaimonStopLog {
    private static final ArrayBlockingQueue<Event> EVENTS = new ArrayBlockingQueue<>(256);
    private static final AtomicLong DROPPED = new AtomicLong();
    private static final Object DISPATCHER_LOCK = new Object();
    private static volatile boolean dispatcherRunning = false;

    private PaimonStopLog() {}

    /** 确保分发线程正在运行。空闲 60 秒后自动退出，允许 ClassLoader 被回收。 */
    private static void ensureDispatcher() {
        synchronized (DISPATCHER_LOCK) {
            if (dispatcherRunning) return;
            dispatcherRunning = true;
            Thread dispatcher = new Thread(() -> {
                try {
                    while (true) {
                        Event event = EVENTS.poll(60, java.util.concurrent.TimeUnit.SECONDS);
                        if (event == null) break; // 空闲 60s，退出以允许 ClassLoader 回收
                        try {
                            String message = event.message;
                            if (event.failure != null) {
                                StringWriter stack = new StringWriter();
                                event.failure.printStackTrace(new PrintWriter(stack));
                                message += " cause=" + stack;
                            }
                            if (event.error) {
                                event.log.error(message);
                            } else if (event.warn) {
                                event.log.warn(message);
                            } else {
                                event.log.info(message);
                            }
                        } catch (Throwable ignored) { /* 观察失败不能影响关闭终态；下一事件继续尝试。 */ }
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                } finally {
                    synchronized (DISPATCHER_LOCK) {
                        dispatcherRunning = false;
                    }
                }
            }, "paimon-stop-log-dispatcher");
            dispatcher.setDaemon(true);
            dispatcher.start();
        }
    }

    public static void offer(Log log, String message, Throwable failure) {
        offer(log, message, failure, false, false);
    }

    /** 失败类事件：队列与丢弃语义不变，仅以 error 级别输出，保证关闭失败即时可见。 */
    public static void offerError(Log log, String message, Throwable failure) {
        offer(log, message, failure, true, false);
    }

    public static void offerWarn(Log log, String message, Throwable failure) {
        offer(log, message, failure, false, true);
    }

    private static void offer(Log log, String message, Throwable failure, boolean error, boolean warn) {
        if (log != null) {
            ensureDispatcher();
            if (!EVENTS.offer(new Event(log, message, failure, error, warn))) {
                DROPPED.incrementAndGet();
            }
        }
    }
    public static long droppedCount() { return DROPPED.get(); }
    static int queuedCount() { return EVENTS.size(); }
    private static final class Event {
        final Log log; final String message; final Throwable failure; final boolean error; final boolean warn;
        Event(Log log, String message, Throwable failure, boolean error, boolean warn) {
            this.log = log; this.message = message; this.failure = failure; this.error = error; this.warn = warn;
        }
    }
}
