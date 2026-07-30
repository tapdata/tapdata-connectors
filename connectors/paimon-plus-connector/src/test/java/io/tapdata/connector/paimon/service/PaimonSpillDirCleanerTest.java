package io.tapdata.connector.paimon.service;

import org.apache.paimon.disk.IOManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PaimonSpillDirCleanerTest {

    @TempDir
    java.nio.file.Path tempDir;

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
            PaimonSpillDirCleaner.unregisterLiveDirs(spillDirs);
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
}
