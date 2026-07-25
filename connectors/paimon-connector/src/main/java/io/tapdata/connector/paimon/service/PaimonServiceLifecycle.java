package io.tapdata.connector.paimon.service;

/**
 * Linearizes service ingress, stop publication, and external callback start.
 *
 * <p>This class deliberately owns no Paimon resource and invokes no external callback. Callers may
 * therefore wait for quiescence without holding a table, writer, or coordinator lock.
 */
final class PaimonServiceLifecycle {

    enum State {
        NEW,
        RUNNING,
        STOPPING,
        FAILED,
        CLOSED
    }

    private State state = State.NEW;
    private Throwable firstFailure;
    private Throwable terminalOutcome;
    private int activeIngress;
    private int activeConsumers;

    synchronized State state() {
        return state;
    }

    synchronized void publishRunning() {
        if (state != State.NEW) {
            throw new IllegalStateException(
                    "Paimon service cannot enter RUNNING from " + state);
        }
        state = State.RUNNING;
    }

    synchronized Ingress enter(String operation) throws Exception {
        if (state == State.RUNNING) {
            activeIngress++;
            return new Ingress(this);
        }
        throwRejected(operation);
        throw new AssertionError("unreachable");
    }

    synchronized boolean beginStopping() {
        if (state == State.RUNNING) {
            state = State.STOPPING;
            notifyAll();
            return true;
        }
        return false;
    }

    synchronized Throwable fail(Throwable failure) {
        if (failure == null) {
            throw new IllegalArgumentException("Lifecycle failure must not be null");
        }
        if (firstFailure == null) {
            firstFailure = failure;
        }
        if (state != State.CLOSED) {
            state = State.FAILED;
        }
        notifyAll();
        return firstFailure;
    }

    synchronized Throwable firstFailure() {
        return firstFailure;
    }

    synchronized ConsumerPermit tryStartConsumer(
            boolean stopDrain, Runnable markConsumerStarted) {
        if (markConsumerStarted == null) {
            throw new IllegalArgumentException("Consumer-start marker must not be null");
        }
        boolean permitted =
                firstFailure == null
                        && ((!stopDrain && state == State.RUNNING)
                                || (stopDrain && state == State.STOPPING));
        if (!permitted) {
            return null;
        }
        markConsumerStarted.run();
        activeConsumers++;
        return new ConsumerPermit(this);
    }

    synchronized void awaitQuiescence() throws InterruptedException {
        while (!isQuiescentLocked()) {
            wait();
        }
    }

    synchronized boolean isQuiescent() {
        return isQuiescentLocked();
    }

    synchronized int activeIngressCount() {
        return activeIngress;
    }

    synchronized int activeConsumerCount() {
        return activeConsumers;
    }

    synchronized Throwable publishClosed(Throwable outcome) {
        if (state == State.CLOSED) {
            return terminalOutcome;
        }
        terminalOutcome = outcome;
        state = State.CLOSED;
        notifyAll();
        return terminalOutcome;
    }

    synchronized Throwable terminalOutcome() {
        return terminalOutcome;
    }

    private boolean isQuiescentLocked() {
        return activeIngress == 0 && activeConsumers == 0;
    }

    private void releaseIngress() {
        synchronized (this) {
            if (activeIngress <= 0) {
                throw new IllegalStateException("No active service ingress to release");
            }
            activeIngress--;
            if (isQuiescentLocked()) {
                notifyAll();
            }
        }
    }

    private void releaseConsumer() {
        synchronized (this) {
            if (activeConsumers <= 0) {
                throw new IllegalStateException("No active callback Consumer to release");
            }
            activeConsumers--;
            if (isQuiescentLocked()) {
                notifyAll();
            }
        }
    }

    private void throwRejected(String operation) throws Exception {
        if (firstFailure != null) {
            rethrow(firstFailure);
        }
        throw new IllegalStateException(
                "Paimon service rejects " + operation + " while lifecycle is " + state);
    }

    private static void rethrow(Throwable failure) throws Exception {
        if (failure instanceof Exception) {
            throw (Exception) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new IllegalStateException("Paimon service failed", failure);
    }

    static final class Ingress implements AutoCloseable {
        private PaimonServiceLifecycle owner;

        private Ingress(PaimonServiceLifecycle owner) {
            this.owner = owner;
        }

        @Override
        public synchronized void close() {
            PaimonServiceLifecycle current = owner;
            if (current != null) {
                owner = null;
                current.releaseIngress();
            }
        }
    }

    static final class ConsumerPermit implements AutoCloseable {
        private PaimonServiceLifecycle owner;

        private ConsumerPermit(PaimonServiceLifecycle owner) {
            this.owner = owner;
        }

        @Override
        public synchronized void close() {
            PaimonServiceLifecycle current = owner;
            if (current != null) {
                owner = null;
                current.releaseConsumer();
            }
        }
    }
}
