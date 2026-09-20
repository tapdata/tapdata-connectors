package io.tapdata.connector.paimon.fs;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PaimonFileSystemInitializationTest {
    @Test
    void initializationMustPreserveCallerIdentityClassLoaderAndFailure() throws Exception {
        UserGroupInformation user = UserGroupInformation.createRemoteUser("paimon-fs-test-user");
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        ThreadGroup callerGroup = Thread.currentThread().getThreadGroup();
        IOException expected = new IOException("initialization failed");

        user.doAs((PrivilegedExceptionAction<Void>) () -> {
            assertSame(expected, assertThrows(IOException.class, () ->
                    PaimonFileSystemInitialization.run(() -> {
                        assertEquals(user, UserGroupInformation.getCurrentUser());
                        assertSame(loader, Thread.currentThread().getContextClassLoader());
                        assertFalse(callerGroup == Thread.currentThread().getThreadGroup());
                        throw expected;
                    })));
            return null;
        });

        Thread.currentThread().interrupt();
        try {
            PaimonFileSystemInitialization.run(() ->
                    assertTrue(Thread.currentThread().isInterrupted()));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void interruptedCallerMustWaitForInitializationAndRestoreInterrupt() throws Exception {
        CountDownLatch initialized = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean returned = new AtomicBoolean();
        AtomicBoolean restoredInterrupt = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread caller = new Thread(() -> {
            try {
                PaimonFileSystemInitialization.run(() -> {
                    initialized.countDown();
                    boolean wasInterrupted = false;
                    while (true) {
                        try {
                            release.await();
                            break;
                        } catch (InterruptedException ignored) {
                            wasInterrupted = true;
                            interrupted.countDown();
                        }
                    }
                    if (wasInterrupted) {
                        Thread.currentThread().interrupt();
                    }
                });
                restoredInterrupt.set(Thread.currentThread().isInterrupted());
            } catch (Throwable error) {
                failure.set(error);
            } finally {
                returned.set(true);
            }
        }, "paimon-fs-interrupted-caller-test");
        caller.setDaemon(true);
        caller.start();
        try {
            assertTrue(initialized.await(5L, TimeUnit.SECONDS));
            caller.interrupt();
            assertTrue(interrupted.await(5L, TimeUnit.SECONDS));
            assertFalse(returned.get(), "初始化尚未结束，调用方不能留下逃逸的客户端创建任务");
        } finally {
            release.countDown();
            caller.join(5_000L);
        }
        assertFalse(caller.isAlive());
        assertNull(failure.get());
        assertTrue(restoredInterrupt.get());
    }
}
