package io.tapdata.connector.paimon.util;

import org.apache.paimon.disk.BufferFileWriter;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.disk.IOManagerImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.Comparator;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Spill 所有权协议所依赖的平台语义（characterization tests）。
 *
 * <p>这些用例不测试 Connector 逻辑，而是把 JDK FileLock / POSIX fcntl 记录锁、RuntimeMXBean、
 * POSIX 目录操作与 Paimon 1.3.2 IOManager 的实际行为固定为可执行事实。任一用例失败都意味着
 * SPEC-paimon-spill-owner-isolation 的前提改变，必须先修订 Spec，而不是修改断言。
 */
@DisabledOnOs(OS.WINDOWS)
class PaimonSpillPlatformContractTest {

    /** C1：同 JVM 对已持锁文件的探测抛 OverlappingFileLockException，关闭探测 channel 会释放本进程的 OS 锁。 */
    @Test
    void sameJvmProbeCloseReleasesOwnerProcessLock(@TempDir Path dir) throws Exception {
        Path lockFile = dir.resolve("owner.lock");
        try (FileChannel owner = FileChannel.open(lockFile, CREATE_NEW, WRITE)) {
            FileLock ownerLock = owner.tryLock();
            assertNotNull(ownerLock);
            assertFalse(SpillProbeProcess.otherProcessAcquires(lockFile), "owner 持锁期间其他进程必须拿不到锁");

            // 与 develop PaimonSpillDirCleaner.OwnerLock.tryAcquire 相同：JVM 级 FileLockTable 拒绝后关闭 channel。
            try (FileChannel probe = FileChannel.open(lockFile, READ, WRITE)) {
                assertThrows(OverlappingFileLockException.class, probe::tryLock);
            }

            assertTrue(ownerLock.isValid(), "JDK 视角下 owner 锁仍然有效，owner 无法察觉锁已丢失");
            assertTrue(SpillProbeProcess.otherProcessAcquires(lockFile),
                    "close(2) 释放本进程在该 inode 上的全部 POSIX 记录锁");
        }
    }

    /** C2：其他进程的失败探测与 close 只影响它自己，不释放 owner 进程的锁。 */
    @Test
    void otherProcessProbeDoesNotReleaseOwnerLock(@TempDir Path dir) throws Exception {
        Path lockFile = dir.resolve("owner.lock");
        try (FileChannel owner = FileChannel.open(lockFile, CREATE_NEW, WRITE);
             FileLock lock = owner.tryLock()) {
            assertNotNull(lock);
            assertFalse(SpillProbeProcess.otherProcessAcquires(lockFile));
            assertFalse(SpillProbeProcess.otherProcessAcquires(lockFile),
                    "前一个外部进程关闭自己的 fd 后，owner 的锁必须仍然有效");
        }
    }

    /**
     * C3：RuntimeMXBean.getName() 在同一 JVM 内不稳定（随主机名解析结果变化），pid 与启动时间稳定。
     * jvmId 只能由后两者派生。
     */
    @Test
    void runtimeNameDependsOnHostResolutionButPidAndStartTimeDoNot(@TempDir Path dir) throws Exception {
        String hostname = SpillProbeProcess.localHostName();
        assumeFalse(hostname.isEmpty() || "localhost".equals(hostname), "主机名为 localhost 时无法区分两种结果");
        Path hosts = Files.write(dir.resolve("hosts"), new byte[0]);
        try (SpillProbeProcess.Handle child = SpillProbeProcess.start(
                Collections.singletonList("-Djdk.net.hosts.file=" + hosts),
                "runtime-name", hosts.toString(), hostname)) {
            String before = value(child.expectLine("identity-before="));
            String resolved = value(child.expectLine("resolved="));
            String unresolved = value(child.expectLine("unresolved="));
            String after = value(child.expectLine("identity-after="));
            child.awaitSuccess();

            String pid = before.substring(0, before.indexOf('|'));
            assertEquals(pid + "@" + hostname, resolved);
            assertEquals(pid + "@localhost", unresolved, "解析失败时 VMManagementImpl.getVmId 退回 localhost");
            assertNotEquals(resolved, unresolved, "同一 JVM 两次 getName() 结果不同");
            assertEquals(before, after, "pid 与 JVM 启动时间在进程生命周期内不变");
        }
    }

    /** C4：父目录被外部回收者删除后，O_CREAT|O_EXCL 与 mkdir 都以 ENOENT 失败，owner 需要有界重试。 */
    @Test
    void exclusiveCreateFailsWithNoSuchFileWhenParentIsRemoved(@TempDir Path dir) throws Exception {
        Path namespace = Files.createDirectory(dir.resolve("namespace"));
        Path lease = Files.createDirectory(namespace.resolve("lease"));
        Files.delete(lease);
        assertThrows(NoSuchFileException.class,
                () -> FileChannel.open(lease.resolve("owner.lock"), CREATE_NEW, WRITE).close());
        Files.delete(namespace);
        assertThrows(NoSuchFileException.class, () -> Files.createDirectory(namespace.resolve("lease-2")));
    }

    /** C5：rmdir 只删除空目录（ENOTEMPTY），对已删除目录报 ENOENT；owner 迟到的 rmdir 必须把它视为已完成。 */
    @Test
    void rmdirRemovesOnlyEmptyDirectoriesAndReportsMissingOnes(@TempDir Path dir) throws Exception {
        Path lease = Files.createDirectory(dir.resolve("lease"));
        Path lockFile = Files.write(lease.resolve("owner.lock"), new byte[0]);
        assertThrows(DirectoryNotEmptyException.class, () -> Files.delete(lease));
        Files.delete(lockFile);
        Files.delete(lease);
        assertThrows(NoSuchFileException.class, () -> Files.delete(lease));
    }

    /** C6：不带 CREATE 打开缺失的锁文件以 ENOENT 失败，且不会创建新 inode。 */
    @Test
    void openWithoutCreateNeverRecreatesLockFile(@TempDir Path dir) {
        Path lockFile = dir.resolve("owner.lock");
        assertThrows(NoSuchFileException.class,
                () -> FileChannel.open(lockFile, READ, WRITE, LinkOption.NOFOLLOW_LINKS).close());
        assertFalse(Files.exists(lockFile, LinkOption.NOFOLLOW_LINKS));
    }

    /**
     * C7：owner 持锁时 unlink 再释放，已打开旧文件的迟到回收者只能锁住已 unlink 的 inode；
     * 按路径复核（存在性 / fileKey）必然失败，因此不会删除任何内容。
     */
    @Test
    void unlinkBeforeReleaseLeavesLateOpenerWithUnlinkedInodeOnly(@TempDir Path dir) throws Exception {
        Path lockFile = dir.resolve("owner.lock");
        FileChannel owner = FileChannel.open(lockFile, CREATE_NEW, WRITE);
        try {
            FileLock lock = owner.tryLock();
            assertNotNull(lock);
            assertNotNull(Files.readAttributes(lockFile, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS).fileKey(),
                    "协议用 fileKey 复核；返回 null 的文件系统必须关闭回收");
            try (SpillProbeProcess.Handle late = SpillProbeProcess.start(
                    Collections.emptyList(), "open-then-lock", lockFile.toString())) {
                late.expectLine("opened");
                Files.delete(lockFile);
                lock.release();
                owner.close();
                late.send("go");
                assertEquals("acquired=true exists=false", late.expectLine("acquired="));
                late.awaitSuccess();
            }
        } finally {
            owner.close();
        }
        assertThrows(NoSuchFileException.class, () -> FileChannel.open(lockFile, READ, WRITE).close());
    }

    /** C8：FileStore.type() 可读取；实际字符串作为文件系统白名单的证据输出。 */
    @Test
    void fileStoreTypeIsObservableForTemporaryRoot(@TempDir Path dir) throws IOException {
        String type = Files.getFileStore(dir).type();
        assertNotNull(type);
        assertFalse(type.trim().isEmpty());
        System.out.println("[spill-platform] os=" + System.getProperty("os.name")
                + " jdk=" + System.getProperty("java.version") + " FileStore.type=" + type);
    }

    /** C9：Paimon 1.3.2 接受重复根目录，并为每个条目各建一个 paimon-io 目录；lease 协议必须先去重。 */
    @Test
    void paimonIoManagerAcceptsDuplicateRoots(@TempDir Path dir) throws Exception {
        IOManagerImpl ioManager = (IOManagerImpl) IOManager.create(new String[] {dir.toString(), dir.toString()});
        File[] spill = ioManager.getSpillingDirectories();
        try {
            assertEquals(2, spill.length);
            assertNotEquals(spill[0], spill[1]);
            assertEquals(spill[0].getParentFile().getCanonicalFile(), spill[1].getParentFile().getCanonicalFile());
        } finally {
            ioManager.close();
        }
        assertFalse(spill[0].exists());
        assertFalse(spill[1].exists());
    }

    /**
     * C10：Paimon 1.3.2 FileChannelManagerImpl 只在构造时 mkdirs；目录被删后不重建，后续 spill 以
     * 与现场完全相同的异常形态失败（AbstractFileIOChannel:62 包装 FileNotFoundException）。
     */
    @Test
    void paimonIoManagerNeverRecreatesDeletedSpillDirectory(@TempDir Path dir) throws Exception {
        IOManagerImpl ioManager = (IOManagerImpl) IOManager.create(dir.toString());
        try {
            File spill = ioManager.getSpillingDirectories()[0];
            BufferFileWriter healthy = ioManager.createBufferFileWriter(ioManager.createChannel());
            healthy.closeAndDelete();

            deleteTree(spill.toPath());

            IOException failure = assertThrows(IOException.class,
                    () -> ioManager.createBufferFileWriter(ioManager.createChannel()));
            assertFieldSignature(failure, spill);
            assertFalse(spill.exists(), "Paimon 不会为已删除的 spill 目录补建");
        } finally {
            ioManager.close();
        }
    }

    /** 现场栈：Channel to path '.../paimon-io-UUID/xxx.channel' could not be opened. ← (No such file or directory) */
    static void assertFieldSignature(IOException failure, File spillDir) {
        String message = failure.getMessage();
        assertTrue(message.startsWith("Channel to path '" + spillDir.getPath() + File.separator), message);
        assertTrue(message.endsWith(".channel' could not be opened."), message);
        assertTrue(failure.getCause() instanceof FileNotFoundException, String.valueOf(failure.getCause()));
        assertTrue(failure.getCause().getMessage().endsWith("(No such file or directory)"),
                failure.getCause().getMessage());
    }

    static void deleteTree(Path root) throws IOException {
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : (Iterable<Path>) paths.sorted(Comparator.reverseOrder())::iterator) {
                Files.delete(path);
            }
        }
    }

    private static String value(String line) {
        return line.substring(line.indexOf('=') + 1);
    }
}
