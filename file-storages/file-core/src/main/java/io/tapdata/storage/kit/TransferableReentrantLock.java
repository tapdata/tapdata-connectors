package io.tapdata.storage.kit;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A single-permit lock that supports reentrant acquisition and release from a
 * different thread. The latter is needed for streams that are opened by a
 * source thread and closed by a downstream thread.
 */
public final class TransferableReentrantLock {

    private final Semaphore semaphore = new Semaphore(1, true);
    private Thread owner;
    private int holdCount;

    public Permit acquireUninterruptibly() {
        Thread currentThread = Thread.currentThread();
        synchronized (this) {
            if (owner == currentThread) {
                holdCount++;
                return new Permit(this);
            }
        }

        semaphore.acquireUninterruptibly();
        synchronized (this) {
            owner = currentThread;
            holdCount = 1;
        }
        return new Permit(this);
    }

    private void release() {
        synchronized (this) {
            if (holdCount <= 0) {
                throw new IllegalStateException("Lock permit has already been released");
            }
            holdCount--;
            if (holdCount == 0) {
                owner = null;
                semaphore.release();
            }
        }
    }

    public static final class Permit {
        private final TransferableReentrantLock lock;
        private final AtomicBoolean released = new AtomicBoolean();

        private Permit(TransferableReentrantLock lock) {
            this.lock = lock;
        }

        public void release() {
            if (released.compareAndSet(false, true)) {
                lock.release();
            }
        }
    }
}
