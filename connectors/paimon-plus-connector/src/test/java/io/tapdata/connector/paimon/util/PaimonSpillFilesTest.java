package io.tapdata.connector.paimon.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * owner 与回收者共用的删除语义。迁移自 T10 删除的平铺扫描用例：符号链接不跟随、只统计普通文件字节、
 * 深层目录迭代删除、部分失败不计为成功。
 */
@DisabledOnOs(OS.WINDOWS)
class PaimonSpillFilesTest {

    @Test
    void nestedSymbolicLinkIsDeletedWithoutFollowing(@TempDir Path base) throws Exception {
        Path outside = Files.createDirectory(base.resolve("outside"));
        Path keep = Files.write(outside.resolve("keep.sst"), new byte[] {1, 2, 3});
        Path tree = Files.createDirectory(base.resolve("io"));
        assumeTrue(link(tree.resolve("outside-link"), outside));

        PaimonSpillFiles.DeletionResult result = PaimonSpillFiles.deleteTree(tree, PaimonSpillFiles.FILES_DELETE);

        assertTrue(result.success);
        assertFalse(Files.exists(tree));
        assertTrue(Files.exists(keep), "符号链接目标不得被删除");
    }

    @Test
    void onlyRegularFileBytesAreCounted(@TempDir Path base) throws Exception {
        Path tree = Files.createDirectory(base.resolve("io"));
        Files.write(tree.resolve("one.sst"), new byte[] {1, 2, 3});
        Files.write(Files.createDirectory(tree.resolve("sub")).resolve("two.sst"), new byte[] {4, 5, 6, 7});
        link(tree.resolve("link"), base);

        assertEquals(7L, PaimonSpillFiles.deleteTree(tree, PaimonSpillFiles.FILES_DELETE).bytesDeleted);
    }

    @Test
    void deeplyNestedTreeIsDeletedIteratively(@TempDir Path base) throws Exception {
        Path tree = Files.createDirectory(base.resolve("io"));
        Path nested = tree;
        for (int depth = 0; depth < 256; depth++) {
            nested = Files.createDirectory(nested.resolve("d"));
        }
        Files.write(nested.resolve("deep.sst"), new byte[] {1});

        assertTrue(PaimonSpillFiles.deleteTree(tree, PaimonSpillFiles.FILES_DELETE).success);
        assertFalse(Files.exists(tree));
    }

    @Test
    void partialFailureIsNotSuccessAndCanBeRetried(@TempDir Path base) throws Exception {
        Path tree = Files.createDirectory(base.resolve("io"));
        Path stuck = Files.write(tree.resolve("stuck.sst"), new byte[] {1});
        Files.write(tree.resolve("other.sst"), new byte[] {2});

        PaimonSpillFiles.DeletionResult failed = PaimonSpillFiles.deleteTree(tree, path -> {
            if (path.equals(stuck)) {
                throw new IOException("injected");
            }
            Files.delete(path);
        });

        assertFalse(failed.success);
        assertFalse(failed.failures.isEmpty(), "失败明细必须记录到具体条目");
        assertEquals(stuck, failed.failures.get(0).path, "首个失败必须是删除被注入失败的文件");
        assertTrue(failed.failureSummaries().get(0).contains("injected"), "摘要须含失败原因");
        assertTrue(Files.exists(stuck));
        assertTrue(PaimonSpillFiles.deleteTree(tree, PaimonSpillFiles.FILES_DELETE).success);
    }

    @Test
    void absentTreeCountsAsDeletedOnlyForTheLeaseProtocol(@TempDir Path base) {
        Path absent = base.resolve("absent");
        assertTrue(PaimonSpillFiles.deleteTreeIfPresent(absent, PaimonSpillFiles.FILES_DELETE).success);
        assertFalse(PaimonSpillFiles.deleteTree(absent, PaimonSpillFiles.FILES_DELETE).success);
    }

    @Test
    void emptyDirectoryRemovalFollowsRmdirSemantics(@TempDir Path base) throws Exception {
        Path directory = Files.createDirectory(base.resolve("lease"));
        Path child = Files.write(directory.resolve("owner.lock"), new byte[0]);
        assertEquals(PaimonSpillFiles.EmptyDirectoryRemoval.KEPT, PaimonSpillFiles.removeEmptyDirectory(directory));
        Files.delete(child);
        assertEquals(PaimonSpillFiles.EmptyDirectoryRemoval.REMOVED, PaimonSpillFiles.removeEmptyDirectory(directory));
        assertEquals(PaimonSpillFiles.EmptyDirectoryRemoval.ABSENT, PaimonSpillFiles.removeEmptyDirectory(directory));
    }

    private static boolean link(Path link, Path target) {
        try {
            Files.createSymbolicLink(link, target);
            return true;
        } catch (IOException | UnsupportedOperationException unsupported) {
            return false;
        }
    }
}
