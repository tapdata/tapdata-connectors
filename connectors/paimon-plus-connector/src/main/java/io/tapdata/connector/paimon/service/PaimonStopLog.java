package io.tapdata.connector.paimon.service;

import io.tapdata.entity.logger.Log;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/** 类加载器内共享的有界观察队列；日志后端阻塞不能拖住 STOP caller/supervisor。 */
public final class PaimonStopLog {
    private static final ArrayBlockingQueue<Event> EVENTS = new ArrayBlockingQueue<>(256);
    private static final AtomicLong DROPPED = new AtomicLong();
    static {
        Thread dispatcher = new Thread(() -> {
            while (true) {
                try {
                    Event event = EVENTS.take();
                    String message = event.message;
                    if (event.failure != null) {
                        StringWriter stack = new StringWriter();
                        event.failure.printStackTrace(new PrintWriter(stack));
                        message += " cause=" + stack;
                    }
                    event.log.info(message);
                } catch (Throwable ignored) { /* 观察失败不能影响关闭终态；下一事件继续尝试。 */ }
            }
        }, "paimon-stop-log-dispatcher");
        dispatcher.setDaemon(true);
        dispatcher.start();
    }
    private PaimonStopLog() {}
    public static void offer(Log log, String message, Throwable failure) {
        if (log != null && !EVENTS.offer(new Event(log, message, failure))) { DROPPED.incrementAndGet(); }
    }
    public static long droppedCount() { return DROPPED.get(); }
    static int queuedCount() { return EVENTS.size(); }
    private static final class Event {
        final Log log; final String message; final Throwable failure;
        Event(Log log, String message, Throwable failure) {
            this.log = log; this.message = message; this.failure = failure;
        }
    }
}
