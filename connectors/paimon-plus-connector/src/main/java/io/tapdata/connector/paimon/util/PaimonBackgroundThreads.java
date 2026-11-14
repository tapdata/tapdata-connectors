package io.tapdata.connector.paimon.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/** ClassLoader-local workers must outlive individual task ThreadGroups, but keep caller TCCL. */
public final class PaimonBackgroundThreads implements ThreadFactory {
    private final ThreadGroup group;
    private final String prefix;
    private final AtomicInteger sequence = new AtomicInteger();

    public PaimonBackgroundThreads(String prefix) {
        this.prefix = prefix;
        ThreadGroup root = Thread.currentThread().getThreadGroup();
        while (root.getParent() != null) {
            root = root.getParent();
        }
        group = root;
    }

    @Override
    public Thread newThread(Runnable task) {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Thread worker = new Thread(group, task, prefix + "-"
                + Integer.toHexString(System.identityHashCode(loader)) + "-" + sequence.incrementAndGet());
        worker.setDaemon(true);
        worker.setContextClassLoader(loader);
        return worker;
    }
}
