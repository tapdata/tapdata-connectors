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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
            visitor.record(root, traversalFailure);
        }
        boolean rootDeleted = Files.notExists(root, LinkOption.NOFOLLOW_LINKS);
        return new DeletionResult(!visitor.failed && rootDeleted, visitor.bytesDeleted, visitor.failures);
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

    /** 单个条目的删除失败：路径与原因。收集后由调用方在合适的时机（锁外或既有审计点）记录。 */
    static final class DeleteFailure {
        final Path path;
        final Throwable failure;

        DeleteFailure(Path path, Throwable failure) {
            this.path = path;
            this.failure = failure;
        }
    }

    static final class DeletionResult {
        /** 最多保留的失败明细条数；更多失败仍会置 unsuccessful，只是不再追加明细。 */
        private static final int MAX_RECORDED_FAILURES = 8;

        final boolean success;
        final long bytesDeleted;
        final List<DeleteFailure> failures;

        DeletionResult(boolean success, long bytesDeleted) {
            this(success, bytesDeleted, Collections.emptyList());
        }

        DeletionResult(boolean success, long bytesDeleted, List<DeleteFailure> failures) {
            this.success = success;
            this.bytesDeleted = bytesDeleted;
            this.failures = Collections.unmodifiableList(failures);
        }

        /** 供审计 details 使用的紧凑摘要（{@code path -> message}），空列表表示无失败或未记录。 */
        List<String> failureSummaries() {
            List<String> summaries = new ArrayList<>(failures.size());
            for (DeleteFailure failure : failures) {
                summaries.add(failure.path + " -> " + failure.failure);
            }
            return summaries;
        }
    }

    private static final class DeletingFileVisitor extends SimpleFileVisitor<Path> {
        private final DeleteAction deleteAction;
        private long bytesDeleted;
        private boolean failed;
        private final List<DeleteFailure> failures = new ArrayList<>();

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
            record(file, failure);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException failure) {
            if (failure != null) {
                failed = true;
                record(dir, failure);
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
                record(path, deleteFailure);
            }
        }

        /** 只收集不写日志：本工具被回收者在持锁临界区内调用，日志后端只能由调用方在锁外接触（Spec §4.5）。 */
        private void record(Path path, Throwable failure) {
            if (failures.size() < DeletionResult.MAX_RECORDED_FAILURES) {
                failures.add(new DeleteFailure(path, failure));
            }
        }
    }
}
