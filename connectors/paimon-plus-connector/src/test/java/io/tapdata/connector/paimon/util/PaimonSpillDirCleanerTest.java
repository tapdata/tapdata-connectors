package io.tapdata.connector.paimon.util;

import org.apache.paimon.disk.IOManager;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonSpillDirCleanerTest {

    @TempDir
    java.nio.file.Path tempDir;

    @Test
    void withConfinedTempDirsMustRedirectTempDirsButDelegateChannels() throws Exception {
        org.apache.paimon.disk.IOManager delegate =
                org.mockito.Mockito.mock(org.apache.paimon.disk.IOManager.class);
        org.mockito.Mockito.when(delegate.tempDirs()).thenReturn(new String[] {"/raw-root"});
        org.apache.paimon.disk.FileIOChannel.ID channelId =
                org.mockito.Mockito.mock(org.apache.paimon.disk.FileIOChannel.ID.class);
        org.mockito.Mockito.when(delegate.createChannel()).thenReturn(channelId);

        org.apache.paimon.disk.IOManager confined =
                PaimonSpillDirCleaner.withConfinedTempDirs(
                        delegate, java.util.Collections.singletonList("/spill/paimon-io-1"));

        // tempDirs() is the only redirected view: the GlobalIndexAssigner's rocksdb directory
        // lands inside the registered spill directory, not under the raw root.
        org.junit.jupiter.api.Assertions.assertArrayEquals(
                new String[] {"/spill/paimon-io-1"}, confined.tempDirs());
        // Spill channels still flow through the delegate's real FileChannelManager.
        org.junit.jupiter.api.Assertions.assertSame(channelId, confined.createChannel());
        confined.close();
        org.mockito.Mockito.verify(delegate).close();
        org.mockito.Mockito.verify(delegate, org.mockito.Mockito.never()).tempDirs();
    }

    @Test
    void partialRegistrationMustDeleteOnlyOwnedRootsAndNeverCloseForeignDirectory() throws Exception {
        IOManager foreign = IOManager.create(tempDir.toString());
        List<String> foreignDirs = PaimonSpillDirCleaner.registerLiveDirs(foreign);
        File ownFirst = Files.createDirectory(tempDir.resolve("paimon-io-owned-first")).toFile();
        File ownLast = Files.createDirectory(tempDir.resolve("paimon-io-owned-last")).toFile();
        AtomicInteger unsafeClose = new AtomicInteger();
        org.apache.paimon.disk.IOManagerImpl partiallyOwned = new org.apache.paimon.disk.IOManagerImpl(tempDir.toString()) {
            @Override
            public File[] getSpillingDirectories() {
                return new File[] {ownFirst, new File(foreignDirs.get(0)), ownLast};
            }
            @Override
            public void close() { unsafeClose.incrementAndGet(); }
        };
        try {
            org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                    () -> PaimonSpillDirCleaner.registerCreatedIOManager(partiallyOwned));
            assertEquals(0, unsafeClose.get(), "整体 close 会越权删除冲突目录");
            assertFalse(ownFirst.exists());
            assertFalse(ownLast.exists(), "第一个锁冲突后的自有 root 也必须回滚");
            assertFalse(ownerFile(ownFirst).exists());
            assertFalse(ownerFile(ownLast).exists());
            assertTrue(new File(foreignDirs.get(0)).exists());
            assertTrue(ownerFile(new File(foreignDirs.get(0))).exists());
        } finally {
            foreign.close();
            PaimonSpillDirCleaner.releaseAfterClose(foreignDirs, true);
        }
    }

    @Test
    void failedIoDeletionAfterAccessorsExitMustKeepMarkerAndPermitStaleCleanup() throws Exception {
        PaimonSpillDirCleaner.IOManagerBuildResult built =
                PaimonSpillDirCleaner.resolveAndCreateIOManager(tempDir.toString());
        List<String> paths = built.spillDirs();
        File directory = new File(paths.get(0));
        try {
            assertTrue(ownerFile(directory).exists());
            // 模拟 IOManager 删除抛错但 writer/executor 已退出：仅释放保护，不移除 marker。
            PaimonSpillDirCleaner.releaseAfterClose(paths, false);
            assertTrue(ownerFile(directory).exists());
            assertEquals(1, PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                    new String[] {tempDir.toString()}, 0, null));
            assertFalse(directory.exists());
            assertFalse(ownerFile(directory).exists());
        } finally {
            built.ioManager().close();
            PaimonSpillDirCleaner.releaseAfterClose(paths, true);
        }
    }

    @Test
    void freshUnlockedDirectoryMustRetainOwnerMarkerForLaterCleanup() throws Exception {
        File spillDir = Files.createDirectory(tempDir.resolve("paimon-io-fresh")).toFile();
        File data = Files.write(spillDir.toPath().resolve("fresh.sst"), new byte[] {1})
                .toFile();
        File ownerFile = ownerFile(spillDir);
        assertTrue(ownerFile.createNewFile());

        assertEquals(
                0,
                PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                        new String[] {tempDir.toString()}, 60_000L, null));
        assertTrue(spillDir.exists());
        assertTrue(ownerFile.exists());

        long old = System.currentTimeMillis() - 120_000L;
        assertTrue(data.setLastModified(old));
        assertTrue(spillDir.setLastModified(old));
        assertEquals(
                1,
                PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                        new String[] {tempDir.toString()}, 60_000L, null));
        assertFalse(spillDir.exists());
        assertFalse(ownerFile.exists());
    }

    @Test
    void nestedSymbolicLinkMustNotDeleteExternalTarget() throws Exception {
        Path outsideDir = Files.createDirectory(tempDir.resolve("outside"));
        Path outsideData = Files.write(outsideDir.resolve("keep.sst"), new byte[] {1, 2, 3});
        File spillDir = Files.createDirectory(tempDir.resolve("paimon-io-symlink")).toFile();
        createSymbolicLinkOrSkip(spillDir.toPath().resolve("outside-link"), outsideDir);
        assertTrue(ownerFile(spillDir).createNewFile());

        assertEquals(
                1,
                PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                        new String[] {tempDir.toString()}, 0L, null));

        assertFalse(spillDir.exists());
        assertTrue(Files.isDirectory(outsideDir));
        assertTrue(Files.exists(outsideData));
    }

    @Test
    void topLevelSymbolicLinkMustBeIgnored() throws Exception {
        Path outsideDir = Files.createDirectory(tempDir.resolve("top-level-outside"));
        Path outsideData = Files.write(outsideDir.resolve("keep.sst"), new byte[] {1});
        Path spillLink = tempDir.resolve("paimon-io-top-level-link");
        createSymbolicLinkOrSkip(spillLink, outsideDir);
        File ownerFile =
                new File(
                        tempDir.toFile(),
                        ".paimon-io-top-level-link"
                                + PaimonSpillDirCleaner.OWNER_LOCK_SUFFIX);
        assertTrue(ownerFile.createNewFile());

        assertEquals(
                0,
                PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                        new String[] {tempDir.toString()}, 0L, null));

        assertTrue(Files.isSymbolicLink(spillLink));
        assertTrue(Files.exists(outsideData));
        assertTrue(ownerFile.exists());
    }

    @Test
    void partialDeletionFailureMustRetainOwnerMarkerAndAllowRetry() throws Exception {
        File spillDir = Files.createDirectory(tempDir.resolve("paimon-io-partial")).toFile();
        Path retainedFile =
                Files.write(spillDir.toPath().resolve("retain.sst"), new byte[] {1, 2});
        Files.write(spillDir.toPath().resolve("deleted.sst"), new byte[] {3});
        File ownerFile = ownerFile(spillDir);
        assertTrue(ownerFile.createNewFile());
        AtomicInteger callbackCount = new AtomicInteger();

        int deleted =
                PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                        new String[] {tempDir.toString()},
                        0L,
                        (path, bytes) -> callbackCount.incrementAndGet(),
                        path -> {
                            if (path.equals(retainedFile)) {
                                throw new IOException("injected delete failure");
                            }
                            Files.delete(path);
                        });

        assertEquals(0, deleted);
        assertEquals(0, callbackCount.get());
        assertTrue(spillDir.exists());
        assertTrue(Files.exists(retainedFile));
        assertTrue(ownerFile.exists());

        assertEquals(
                1,
                PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                        new String[] {tempDir.toString()}, 0L, null));
        assertFalse(spillDir.exists());
        assertFalse(ownerFile.exists());
    }

    @Test
    void successfulDeletionMustReportOnlyRegularFileBytes() throws Exception {
        File spillDir = Files.createDirectory(tempDir.resolve("paimon-io-size")).toFile();
        Files.write(spillDir.toPath().resolve("one.sst"), new byte[] {1, 2, 3});
        Files.write(spillDir.toPath().resolve("two.sst"), new byte[] {4, 5, 6, 7});
        assertTrue(ownerFile(spillDir).createNewFile());
        AtomicInteger callbackCount = new AtomicInteger();
        AtomicLong deletedBytes = new AtomicLong(-1L);

        assertEquals(
                1,
                PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                        new String[] {tempDir.toString()},
                        0L,
                        (path, bytes) -> {
                            callbackCount.incrementAndGet();
                            deletedBytes.set(bytes);
                        }));

        assertEquals(1, callbackCount.get());
        assertEquals(7L, deletedBytes.get());
    }

    @Test
    void deeplyNestedDirectoryMustBeDeletedWithoutRecursion() throws Exception {
        File spillDir = Files.createDirectory(tempDir.resolve("paimon-io-deep")).toFile();
        Path nested = spillDir.toPath();
        for (int depth = 0; depth < 256; depth++) {
            nested = Files.createDirectory(nested.resolve("d"));
        }
        Files.write(nested.resolve("deep.sst"), new byte[] {1});
        assertTrue(ownerFile(spillDir).createNewFile());

        assertEquals(
                1,
                PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                        new String[] {tempDir.toString()}, 0L, null));
        assertFalse(spillDir.exists());
    }

    @Test
    void cleanupMustNotDeleteDirectoryLockedByAnotherProcessOwner() throws Exception {
        File spillDir = Files.createDirectory(tempDir.resolve("paimon-io-external")).toFile();
        File data = Files.write(spillDir.toPath().resolve("active.sst"), new byte[] {1, 2, 3})
                .toFile();
        long old = System.currentTimeMillis() - 60_000L;
        assertTrue(data.setLastModified(old));
        assertTrue(spillDir.setLastModified(old));

        File ownerFile = new File(
                tempDir.toFile(), ".paimon-io-external" + PaimonSpillDirCleaner.OWNER_LOCK_SUFFIX);
        try (RandomAccessFile raf = new RandomAccessFile(ownerFile, "rw");
             FileLock ignored = raf.getChannel().lock()) {
            assertEquals(0, PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                    new String[] {tempDir.toString()}, 0L, null));
            assertTrue(spillDir.exists());
        }

        assertEquals(1, PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                new String[] {tempDir.toString()}, 0L, null));
        assertFalse(spillDir.exists());
        assertFalse(ownerFile.exists());
    }

    @Test
    void registeredIoManagerDirectoryMustRemainProtectedUntilUnregistered() throws Exception {
        IOManager ioManager = IOManager.create(new String[] {tempDir.toString()});
        List<String> spillDirs = PaimonSpillDirCleaner.registerLiveDirs(ioManager);
        try {
            assertFalse(spillDirs.isEmpty());
            assertEquals(0, PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                    new String[] {tempDir.toString()}, 0L, null));
            for (String path : spillDirs) {
                assertTrue(new File(path).exists());
            }
        } finally {
            ioManager.close();
            PaimonSpillDirCleaner.releaseAfterClose(spillDirs, true);
        }

        for (String path : spillDirs) {
            File spillDir = new File(path);
            File ownerFile = new File(
                    spillDir.getParentFile(),
                    "." + spillDir.getName() + PaimonSpillDirCleaner.OWNER_LOCK_SUFFIX);
            assertFalse(ownerFile.exists());
        }
    }

    @Test
    void locklessLegacyDirectoryMustNotBeDeletedDuringRollingUpgrade() throws Exception {
        File spillDir = Files.createDirectory(tempDir.resolve("paimon-io-old-version")).toFile();
        File data = Files.write(spillDir.toPath().resolve("possibly-active.sst"), new byte[] {1})
                .toFile();
        long old = System.currentTimeMillis() - 60_000L;
        assertTrue(data.setLastModified(old));
        assertTrue(spillDir.setLastModified(old));

        assertEquals(0, PaimonSpillDirCleaner.cleanupStaleSpillDirs(
                new String[] {tempDir.toString()}, 0L, null));
        assertTrue(spillDir.exists());
    }

    @Test
    void resolveTmpDirsMustFallBackToJavaIoTmpdirThenWorkingDir() {
        String workingDir = new File(".").getAbsolutePath();
        String ioTmpdir = System.getProperty("java.io.tmpdir", workingDir);

        // null / blank configured values fall back to java.io.tmpdir (then working dir).
        assertEquals(ioTmpdir, PaimonSpillDirCleaner.resolveTmpDirs(null));
        assertEquals(ioTmpdir, PaimonSpillDirCleaner.resolveTmpDirs(""));
        assertEquals(ioTmpdir, PaimonSpillDirCleaner.resolveTmpDirs("   "));

        // Non-blank configured values are returned verbatim (multi-path preserved).
        assertEquals("/custom", PaimonSpillDirCleaner.resolveTmpDirs("/custom"));
        assertEquals("/a,/b,/c", PaimonSpillDirCleaner.resolveTmpDirs("/a,/b,/c"));
    }

    @Test
    void splitTmpDirRootsMustDelegateToPaimonSplitPaths() {
        // splitTmpDirRoots is a thin wrapper over Paimon's IOManagerImpl.splitPaths: it splits on
        // comma/path-separator without trimming or dropping empty segments (callers that need clean
        // roots sanitize themselves). Verify the wrapper preserves that contract.
        String[] roots = PaimonSpillDirCleaner.splitTmpDirRoots("/a,/b,/c");
        assertEquals(3, roots.length);
        assertEquals("/a", roots[0]);
        assertEquals("/b", roots[1]);
        assertEquals("/c", roots[2]);

        // Empty input yields an empty array (splitPaths short-circuits length == 0).
        assertEquals(0, PaimonSpillDirCleaner.splitTmpDirRoots("").length);
    }

    private static File ownerFile(File spillDir) {
        return new File(
                spillDir.getParentFile(),
                "." + spillDir.getName() + PaimonSpillDirCleaner.OWNER_LOCK_SUFFIX);
    }

    private static void createSymbolicLinkOrSkip(Path link, Path target) {
        try {
            Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException | IOException | SecurityException failure) {
            Assumptions.assumeTrue(false, "Symbolic links are unavailable: " + failure);
        }
    }
}
