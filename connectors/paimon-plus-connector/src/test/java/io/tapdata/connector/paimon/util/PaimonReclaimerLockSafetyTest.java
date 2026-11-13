package io.tapdata.connector.paimon.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/** Regression for POSIX close releasing another reclaimer's process/inode lock. */
@DisabledOnOs(OS.WINDOWS)
class PaimonReclaimerLockSafetyTest {
    @Test
    void competingReclaimerCannotReleaseOsLock(@TempDir Path root) throws Exception {
        exercise(root, false);
    }

    @Test
    void competingClassLoaderCannotReleaseOsLock(@TempDir Path root) throws Exception {
        exercise(root, true);
    }

    private void exercise(Path root, boolean isolated) throws Exception {
        Path lease = Files.createDirectories(root.resolve(PaimonSpillLease.SUBROOT)
                .resolve(PaimonJvmIdentity.derive(7L, 7L)).resolve("0123456789ab-" + UUID.randomUUID()));
        Path lock = Files.write(lease.resolve("owner.lock"), new byte[0]);
        Path sentinel = Files.write(Files.createDirectory(lease.resolve("io")).resolve("live.channel"), new byte[]{1});
        CountDownLatch deleting = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        CountDownLatch competing = new CountDownLatch(1);
        AtomicReference<Thread> competingThread = new AtomicReference<>();
        PaimonSpillAudit audit = new PaimonSpillAudit((e, f, t) -> { }, System::nanoTime, k -> null);
        PaimonSpillReclaimer first = new PaimonSpillReclaimer(PaimonJvmIdentity::current,
                PaimonSpillReclaimer.DEFAULT_TYPE_PROBE, PaimonSpillReclaimer.DEFAULT_OPENER, path -> {
                    if (path.equals(sentinel)) {
                        deleting.countDown();
                        try {
                            if (!resume.await(20, TimeUnit.SECONDS)) { throw new java.io.IOException("resume timeout"); }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new java.io.IOException(e);
                        }
                    }
                    Files.delete(path);
                }, audit, 64);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        // Open in an external JVM before A locks, just as an owner paused after CREATE_NEW.
        try (URLClassLoader loader = IsolatedConnectorLoader.create();
             SpillProbeProcess.Handle owner = SpillProbeProcess.start(Collections.emptyList(), "open-then-lock", lock.toString())) {
            owner.expectLine("opened");
            Future<?> a = pool.submit(() -> first.reclaim(Collections.singletonList(root.toString())));
            assertTrue(deleting.await(15, TimeUnit.SECONDS));
            Future<?> b = pool.submit(() -> {
                competingThread.set(Thread.currentThread());
                competing.countDown();
                if (isolated) {
                    try {
                        Class<?> driver = loader.loadClass(SpillLoaderDriver.class.getName());
                        driver.getMethod("reclaim", Collection.class, Consumer.class).invoke(null,
                                Collections.singletonList(root.toString()), (Consumer<Path>) p -> { });
                    } catch (Exception e) { throw new IllegalStateException(e); }
                } else {
                    PaimonSpillReclaimer.standard().reclaim(Collections.singletonList(root.toString()));
                }
            });
            assertTrue(competing.await(15, TimeUnit.SECONDS));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
            while (!b.isDone() && competingThread.get().getState() != Thread.State.BLOCKED
                    && System.nanoTime() < deadline) { Thread.sleep(10); }
            boolean competingFinished = b.isDone();
            Thread.State competingState = competingThread.get().getState();
            owner.send("go");
            assertEquals("acquired=false exists=true", owner.expectLine("acquired="),
                    "External owner must still be denied by the real OS lock, not FileLock.isValid()");
            owner.awaitSuccess();
            assertFalse(competingFinished, "B must wait before opening the same inode");
            assertEquals(Thread.State.BLOCKED, competingState);
            assertTrue(Files.exists(sentinel));
            resume.countDown();
            a.get(15, TimeUnit.SECONDS);
            b.get(15, TimeUnit.SECONDS);
            assertFalse(Files.exists(lease));
        } finally {
            resume.countDown();
            pool.shutdownNow();
            assertTrue(pool.awaitTermination(20, TimeUnit.SECONDS));
        }
    }
}
