package io.tapdata.storage.kit;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransferableReentrantLockTest {

    @Test
    void permitsAreReentrantAndCanBeReleasedByAnotherThread() throws Exception {
        TransferableReentrantLock lock = new TransferableReentrantLock();
        TransferableReentrantLock.Permit outer = lock.acquireUninterruptibly();
        TransferableReentrantLock.Permit inner = lock.acquireUninterruptibly();
        CountDownLatch attempted = new CountDownLatch(1);
        CountDownLatch acquired = new CountDownLatch(1);
        AtomicBoolean waiterCompleted = new AtomicBoolean();

        Thread waiter = new Thread(() -> {
            attempted.countDown();
            TransferableReentrantLock.Permit permit = lock.acquireUninterruptibly();
            try {
                waiterCompleted.set(true);
                acquired.countDown();
            } finally {
                permit.release();
            }
        });
        waiter.start();

        assertTrue(attempted.await(1, TimeUnit.SECONDS));
        assertFalse(acquired.await(100, TimeUnit.MILLISECONDS));

        Thread releaser = new Thread(inner::release);
        releaser.start();
        releaser.join(1000);
        assertFalse(acquired.await(100, TimeUnit.MILLISECONDS));

        outer.release();
        assertTrue(acquired.await(1, TimeUnit.SECONDS));
        waiter.join(1000);
        assertTrue(waiterCompleted.get());
    }
}
