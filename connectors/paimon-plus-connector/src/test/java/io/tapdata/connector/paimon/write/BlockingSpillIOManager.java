package io.tapdata.connector.paimon.write;

import org.apache.paimon.disk.BufferFileReader;
import org.apache.paimon.disk.BufferFileWriter;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test fixture: delegates to a real {@link IOManagerImpl} but can park the first spill worker
 * inside {@link #createBufferFileWriter}, modelling a compaction thread blocked in native file
 * IO while the rest of the system tries to shut down. The park deliberately swallows interrupts
 * — exactly like a thread stuck in an uninterruptible filesystem call.
 */
final class BlockingSpillIOManager implements IOManager {

    private final IOManagerImpl delegate;
    private final CountDownLatch spillEntered;
    private final CountDownLatch releaseSpill;
    private volatile String firstSpillStack;
    private final AtomicInteger closeCount = new AtomicInteger();

    BlockingSpillIOManager(String tmpDir, CountDownLatch spillEntered, CountDownLatch releaseSpill) {
        this.delegate = (IOManagerImpl) IOManager.create(tmpDir);
        this.spillEntered = spillEntered;
        this.releaseSpill = releaseSpill;
    }

    /** Stack of the first parked spill call; guards the fixture against silently leaving the
     * real compaction spill path (Spec §7.1). */
    String firstSpillStack() {
        return firstSpillStack;
    }

    void awaitReleaseSwallowingInterrupts() {
        // Model uninterruptible native IO: catch clears the interrupt flag and we keep waiting.
        while (true) {
            try {
                releaseSpill.await();
                return;
            } catch (InterruptedException e) {
                // swallowed on purpose
            }
        }
    }

    org.apache.paimon.disk.IOManagerImpl delegate() {
        return delegate;
    }

    @Override
    public org.apache.paimon.disk.FileIOChannel.ID createChannel() {
        return delegate.createChannel();
    }

    @Override
    public org.apache.paimon.disk.FileIOChannel.ID createChannel(String prefix) {
        return delegate.createChannel(prefix);
    }

    @Override
    public String[] tempDirs() {
        return delegate.tempDirs();
    }

    @Override
    public org.apache.paimon.disk.FileIOChannel.Enumerator createChannelEnumerator() {
        return delegate.createChannelEnumerator();
    }

    @Override
    public BufferFileWriter createBufferFileWriter(org.apache.paimon.disk.FileIOChannel.ID channelID) throws IOException {
        if (firstSpillStack == null) {
            StringBuilder stack = new StringBuilder();
            for (StackTraceElement frame : Thread.currentThread().getStackTrace()) {
                stack.append(frame.getClassName())
                        .append('.')
                        .append(frame.getMethodName())
                        .append('\n');
            }
            firstSpillStack = stack.toString();
        }
        spillEntered.countDown();
        awaitReleaseSwallowingInterrupts();
        return delegate.createBufferFileWriter(channelID);
    }

    @Override
    public BufferFileReader createBufferFileReader(org.apache.paimon.disk.FileIOChannel.ID channelID) throws IOException {
        return delegate.createBufferFileReader(channelID);
    }

    @Override
    public void close() throws Exception {
        closeCount.incrementAndGet();
        delegate.close();
    }

    int closeCount() {
        return closeCount.get();
    }

    File[] spillingDirectories() {
        return delegate.getSpillingDirectories();
    }
}
