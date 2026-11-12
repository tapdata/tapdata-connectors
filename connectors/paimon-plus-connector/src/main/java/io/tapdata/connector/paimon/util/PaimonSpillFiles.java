package io.tapdata.connector.paimon.util;

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Spill owner 与回收者共用的文件系统操作。删除不跟随符号链接：{@link Files#walkFileTree(Path,
 * java.nio.file.FileVisitor)} 默认不跟随链接，{@link Files#delete(Path)} 对符号链接只删除链接本身。
 * https://docs.oracle.com/javase/8/docs/api/java/nio/file/Files.html#delete-java.nio.file.Path-
 */
final class PaimonSpillFiles {

    @FunctionalInterface
    interface DeleteAction {
        void delete(Path path) throws IOException;
    }

    static final DeleteAction FILES_DELETE = Files::delete;

    private PaimonSpillFiles() {
    }

    /** 租约协议使用：目录本就不存在视为已删除（例如 IOManager.close 已删除其下全部内容）。 */
    static DeletionResult deleteTreeIfPresent(Path root, DeleteAction deleteAction) {
        if (Files.notExists(root, LinkOption.NOFOLLOW_LINKS)) {
            return new DeletionResult(true, 0L);
        }
        return deleteTree(root, deleteAction);
    }

    /** 递归删除；任一条目失败或根仍存在即整体不成功（部分删除不计为成功）。 */
    static DeletionResult deleteTree(Path root, DeleteAction deleteAction) {
        DeletingFileVisitor visitor = new DeletingFileVisitor(deleteAction);
        try {
            Files.walkFileTree(root, visitor);
        } catch (IOException | SecurityException traversalFailure) {
            visitor.failed = true;
        }
        boolean rootDeleted = Files.notExists(root, LinkOption.NOFOLLOW_LINKS);
        return new DeletionResult(!visitor.failed && rootDeleted, visitor.bytesDeleted);
    }

    /**
     * rmdir 语义：只删除空目录。返回 {@link EmptyDirectoryRemoval#REMOVED}、{@link EmptyDirectoryRemoval#ABSENT}
     * （ENOENT，已被其他参与者删除）或 {@link EmptyDirectoryRemoval#KEPT}（ENOTEMPTY 或其他错误）。
     */
    static EmptyDirectoryRemoval removeEmptyDirectory(Path directory) {
        try {
            Files.delete(directory);
            return EmptyDirectoryRemoval.REMOVED;
        } catch (NoSuchFileException absent) {
            return EmptyDirectoryRemoval.ABSENT;
        } catch (DirectoryNotEmptyException notEmpty) {
            return EmptyDirectoryRemoval.KEPT;
        } catch (IOException | SecurityException failure) {
            return EmptyDirectoryRemoval.KEPT;
        }
    }

    enum EmptyDirectoryRemoval { REMOVED, ABSENT, KEPT }

    static final class DeletionResult {
        final boolean success;
        final long bytesDeleted;

        DeletionResult(boolean success, long bytesDeleted) {
            this.success = success;
            this.bytesDeleted = bytesDeleted;
        }
    }

    private static final class DeletingFileVisitor extends SimpleFileVisitor<Path> {
        private final DeleteAction deleteAction;
        private long bytesDeleted;
        private boolean failed;

        private DeletingFileVisitor(DeleteAction deleteAction) {
            this.deleteAction = deleteAction;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) {
            long fileBytes = attributes.isRegularFile() ? attributes.size() : 0L;
            delete(file, fileBytes);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException failure) {
            failed = true;
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException failure) {
            if (failure != null) {
                failed = true;
            }
            delete(dir, 0L);
            return FileVisitResult.CONTINUE;
        }

        private void delete(Path path, long fileBytes) {
            try {
                deleteAction.delete(path);
                bytesDeleted = PaimonSpillDirCleaner.saturatedAdd(bytesDeleted, fileBytes);
            } catch (IOException | SecurityException deleteFailure) {
                failed = true;
            }
        }
    }
}
