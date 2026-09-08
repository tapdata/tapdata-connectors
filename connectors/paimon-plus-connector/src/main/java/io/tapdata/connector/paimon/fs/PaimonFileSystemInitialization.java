package io.tapdata.connector.paimon.fs;

import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicLong;

/** 让 Hadoop 客户端初始化时捕获的线程组独立于可被销毁的任务线程组。 */
final class PaimonFileSystemInitialization {
    private static final ThreadGroup THREAD_GROUP = createThreadGroup();
    private static final AtomicLong NEXT_THREAD_ID = new AtomicLong();

    private PaimonFileSystemInitialization() {}

    static void run(Initialization initialization) throws IOException {
        UserGroupInformation callerUser = UserGroupInformation.getCurrentUser();
        boolean initiallyInterrupted = Thread.interrupted();
        boolean interrupted = initiallyInterrupted;
        try {
            FutureTask<Void> task = new FutureTask<>(() -> callerUser.doAs(
                    (PrivilegedExceptionAction<Void>) () -> {
                        if (initiallyInterrupted) {
                            Thread.currentThread().interrupt();
                        }
                        initialization.run();
                        return null;
                    }));
            Thread worker = new Thread(
                    THREAD_GROUP, task, "paimon-filesystem-init-" + NEXT_THREAD_ID.incrementAndGet());
            worker.setDaemon(true);
            worker.setContextClassLoader(Thread.currentThread().getContextClassLoader());
            worker.start();
            while (worker.isAlive()) {
                try {
                    worker.join();
                } catch (InterruptedException failure) {
                    interrupted = true;
                    worker.interrupt();
                }
            }
            try {
                task.get();
            } catch (InterruptedException failure) {
                // worker 已退出，get 不再等待；保留异常路径以防未来修改引入等待。
                interrupted = true;
                InterruptedIOException error = new InterruptedIOException(
                        "Interrupted while obtaining Hadoop initialization result");
                error.initCause(failure);
                throw error;
            } catch (ExecutionException failure) {
                Throwable cause = failure.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                if (cause instanceof Error) {
                    throw (Error) cause;
                }
                throw new IOException("Hadoop FileSystem initialization failed", cause);
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static ThreadGroup createThreadGroup() {
        ThreadGroup root = Thread.currentThread().getThreadGroup();
        while (root.getParent() != null) {
            root = root.getParent();
        }
        return new ThreadGroup(root, "paimon-filesystem-workers");
    }

    @FunctionalInterface
    interface Initialization {
        void run() throws IOException;
    }
}
