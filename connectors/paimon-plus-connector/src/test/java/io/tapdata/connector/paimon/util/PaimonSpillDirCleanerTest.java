package io.tapdata.connector.paimon.util;

import org.apache.paimon.disk.IOManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    /** Spec D6：IOManager 已构造但目录不在租约内时，保留租约与锁，不 close、不递归删除。 */
    @Test
    void ioManagerOutsideItsLeaseIsRetainedWithoutClose() throws Exception {
        Path root = tempDir.resolve("root");
        Path misplaced = Files.createDirectory(tempDir.resolve("misplaced"));
        AtomicInteger closes = new AtomicInteger();
        io.tapdata.connector.paimon.service.PaimonStopResources.Scope scope =
                io.tapdata.connector.paimon.service.PaimonStopResources.Scope.standalone("misplaced");
        try (org.mockito.MockedStatic<IOManager> factory = org.mockito.Mockito.mockStatic(IOManager.class)) {
            factory.when(() -> IOManager.create(org.mockito.ArgumentMatchers.any(String[].class)))
                    .thenAnswer(call -> new org.apache.paimon.disk.IOManagerImpl(misplaced.toString()) {
                        @Override
                        public void close() {
                            closes.incrementAndGet();
                        }
                    });
            assertThrows(IllegalStateException.class,
                    () -> PaimonSpillDirCleaner.resolveAndCreateIOManager(root.toString(), "default.t", scope));
        }
        assertEquals(0, closes.get(), "构造后的登记失败只保留，不关闭 IOManager");
        assertTrue(scope.controller().isRetained());
        Path lockFile = onlyLockFileUnder(root);
        assertFalse(SpillProbeProcess.otherProcessAcquires(lockFile), "保留的租约必须继续持有 OS 锁");
    }

    @Test
    void failedIoDeletionMustRetainOwnerAndRejectStaleCleanup() throws Exception {
        PaimonSpillDirCleaner.IOManagerBuildResult built =
                PaimonSpillDirCleaner.resolveAndCreateIOManager(tempDir.toString(), "default.t");
        List<String> paths = built.spillDirs();
        File directory = new File(paths.get(0));
        try {
            assertTrue(Files.isRegularFile(SpillTestSupport.ownerLockFile(paths.get(0))));
            // V1.2：没有实际删除证明，保留租约与锁，不能依赖启动清理解除占用。
            assertFalse(PaimonSpillDirCleaner.releaseAfterClose(paths, false).complete());
            assertTrue(SpillTestSupport.ownerRegistered(paths.get(0)));
            assertEquals(0, SpillTestSupport.runStartupCleanup(paths, null));
            assertTrue(directory.exists());
            assertFalse(SpillProbeProcess.otherProcessAcquires(SpillTestSupport.ownerLockFile(paths.get(0))));
        } finally {
            built.ioManager().close();
            assertTrue(PaimonSpillDirCleaner.releaseAfterClose(paths, true).complete());
        }
        assertFalse(SpillTestSupport.ownerRegistered(paths.get(0)));
        assertFalse(Files.exists(SpillTestSupport.ownerLockFile(paths.get(0)).getParent()), "退役后租约目录被删除");
    }

    /** Spec D5：释放未登记的非空路径时报告不完整，而不是静默成功。 */
    @Test
    void releasingAnUnregisteredDirectoryIsIncomplete() {
        PaimonSpillDirCleaner.ReleaseResult result = PaimonSpillDirCleaner.releaseAfterClose(
                java.util.Collections.singletonList(tempDir.resolve("paimon-io-unknown").toString()), true);
        assertFalse(result.complete());
        assertTrue(result.failure() instanceof IllegalStateException, String.valueOf(result.failure()));
        assertTrue(PaimonSpillDirCleaner.releaseAfterClose(java.util.Collections.emptyList(), true).complete());
    }

    /** AC-20：重复或互为别名的根只建一个租约，IOManager 得到唯一的根。 */
    @Test
    void duplicateRootsShareOneLeaseRoot() throws Exception {
        PaimonSpillDirCleaner.IOManagerBuildResult built = PaimonSpillDirCleaner.resolveAndCreateIOManager(
                tempDir + "," + tempDir, "default.t");
        try {
            assertEquals(1, built.spillDirs().size());
        } finally {
            built.ioManager().close();
            assertTrue(PaimonSpillDirCleaner.releaseAfterClose(built.spillDirs(), true).complete());
        }
    }

    @Test
    void registeredIoManagerDirectoryMustRemainProtectedUntilUnregistered() throws Exception {
        PaimonSpillDirCleaner.IOManagerBuildResult built =
                PaimonSpillDirCleaner.resolveAndCreateIOManager(tempDir.toString(), "default.t");
        List<String> spillDirs = built.spillDirs();
        try {
            assertFalse(spillDirs.isEmpty());
            assertEquals(0, SpillTestSupport.runStartupCleanup(spillDirs, null));
            for (String path : spillDirs) {
                assertTrue(new File(path).exists());
                assertTrue(SpillTestSupport.ownerRegistered(path));
            }
        } finally {
            built.ioManager().close();
            PaimonSpillDirCleaner.releaseAfterClose(spillDirs, true);
        }

        for (String path : spillDirs) {
            assertFalse(Files.exists(SpillTestSupport.ownerLockFile(path)));
            assertFalse(SpillTestSupport.ownerRegistered(path));
        }
    }

    @Test
    void resolveTmpDirsMustFallBackToJavaIoTmpdir() {
        String ioTmpdir = System.getProperty("java.io.tmpdir", "/tmp");

        // null / blank configured values fall back to java.io.tmpdir (then /tmp).
        assertEquals(ioTmpdir, PaimonSpillDirCleaner.resolveTmpDirs(null));
        assertEquals(ioTmpdir, PaimonSpillDirCleaner.resolveTmpDirs(""));
        assertEquals(ioTmpdir, PaimonSpillDirCleaner.resolveTmpDirs("   "));

        // Valid absolute multi-path values are preserved.
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

    @Test
    void malformedDirectoryListMustFailBeforeCreatingAnyDirectory() {
        Path validRoot = tempDir.resolve("not-created");
        for (String invalid : new String[] {
                "'bucket' = '-1', 'write-buffer-size' = '128mb'",
                ".", "relative", "file:///tmp/spill", "/tmp/spill,", "/tmp/spill,,/tmp/other",
                validRoot + ", 'write-buffer-spillable' = 'true'"}) {
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> PaimonSpillDirCleaner.resolveAndCreateIOManager(invalid, "default.t"), invalid);
            assertTrue(error.getMessage().contains("diskTmpDir"));
            assertFalse(Files.exists(validRoot));
        }
    }

    @Test
    void normalizedMultipleRootsMustOwnAndReleaseRealSpillDirectories() throws Exception {
        Path first = tempDir.resolve("first root");
        Path second = tempDir.resolve("second");
        String configured = " " + first + " , " + second + " ";
        assertEquals(first + "," + second, PaimonSpillDirCleaner.resolveTmpDirs(configured));
        PaimonSpillDirCleaner.IOManagerBuildResult built =
                PaimonSpillDirCleaner.resolveAndCreateIOManager(configured, "default.t");
        try {
            assertEquals(2, built.spillDirs().size());
            assertTrue(Files.isDirectory(first));
            assertTrue(Files.isDirectory(second));
            assertTrue(built.spillDirs().get(0).startsWith(first.toRealPath().resolve("tapdata-paimon-v2").toString()));
            assertTrue(built.spillDirs().get(1).startsWith(second.toRealPath().resolve("tapdata-paimon-v2").toString()));
        } finally {
            built.ioManager().close();
            assertTrue(PaimonSpillDirCleaner.releaseAfterClose(built.spillDirs(), true).complete());
        }
        for (String directory : built.spillDirs()) {
            assertFalse(Files.exists(java.nio.file.Paths.get(directory)));
            assertFalse(Files.exists(SpillTestSupport.ownerLockFile(directory)));
        }
    }

    @Test
    void missingJvmTmpdirMustNotFallBackToWorkingDirectory() {
        String previous = System.getProperty("java.io.tmpdir");
        try {
            System.clearProperty("java.io.tmpdir");
            assertEquals("/tmp", PaimonSpillDirCleaner.resolveTmpDirs(null));
            System.setProperty("java.io.tmpdir", ".");
            assertThrows(IllegalArgumentException.class, () -> PaimonSpillDirCleaner.resolveTmpDirs(null));
        } finally {
            if (previous == null) { System.clearProperty("java.io.tmpdir"); }
            else { System.setProperty("java.io.tmpdir", previous); }
        }
    }

    private static Path onlyLockFileUnder(Path root) throws IOException {
        try (java.util.stream.Stream<Path> paths = Files.walk(root)) {
            List<Path> locks = paths.filter(path -> path.getFileName().toString().equals("owner.lock"))
                    .collect(java.util.stream.Collectors.toList());
            assertEquals(1, locks.size(), String.valueOf(locks));
            return locks.get(0);
        }
    }

}
