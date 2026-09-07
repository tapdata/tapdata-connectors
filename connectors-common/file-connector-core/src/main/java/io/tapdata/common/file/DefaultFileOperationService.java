package io.tapdata.common.file;

import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.file.operation.*;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultFileOperationService implements TapFileOperationService {
    private static final int MAX_BATCH_FILES = 100;
    private final FileStorageSessionManager sessions;

    public DefaultFileOperationService() {
        this(new DefaultFileStorageSessionManager());
    }

    public DefaultFileOperationService(FileStorageSessionManager sessions) {
        this.sessions = sessions;
    }

    @Override
    public FileOperationResult copy(FileCopyRequest request) {
        long start = System.currentTimeMillis();
        FileStorageSession source = null;
        FileStorageSession target = null;
        try {
            source = sessions.retain(request.getSource());
            target = sessions.retain(request.getTarget());
            String sourcePath = FilePathPolicy.resolveRoot(request.getSource().getRootPath(), request.getSourcePath());
            String targetPath = FilePathPolicy.resolveRoot(request.getTarget().getRootPath(), request.getTargetPath());
            TapFile sourceFile = source.getStorage().getFile(sourcePath);
            if (sourceFile == null || !Integer.valueOf(TapFile.TYPE_FILE).equals(sourceFile.getType())) {
                throw error(FileOperationErrorCode.FILE_NOT_FOUND, "source file does not exist", sourcePath);
            }
            if (request.isDryRun()) {
                return result(FileOperationStatus.DRY_RUN, request, sourceFile, start);
            }

            TapFile existing = target.getStorage().getFile(targetPath);
            if (existing != null && !request.isOverwrite()) {
                if (sameSize(sourceFile, existing) && request.getVerifyMode() != FileVerifyMode.CHECKSUM) {
                    return result(FileOperationStatus.REUSED, request, existing, start);
                }
                throw error(FileOperationErrorCode.FILE_TARGET_CONFLICT,
                        "target file already exists with different content", targetPath);
            }

            EnumSet<FileStorageCapability> capabilities = target.getStorage().capabilities();
            if (!capabilities.contains(FileStorageCapability.ATOMIC_RENAME)) {
                throw error(FileOperationErrorCode.FILE_UNSUPPORTED_OPERATION,
                        "target storage does not support atomic rename", targetPath);
            }
            String tempPath = ".tapdata-tmp/" + Thread.currentThread().getId() + "/" +
                    Math.abs(System.nanoTime()) + ".part";
            try {
                final String finalTempPath = tempPath;
                final TapFileStorage targetStorage = target.getStorage();
                source.getStorage().readFile(sourcePath, input -> {
                    try {
                        targetStorage.saveFile(finalTempPath, input, true);
                    } catch (Exception e) {
                        throw new FileOperationException(FileOperationErrorCode.FILE_WRITE_FAILED,
                                "failed to write temporary file", e, "write", finalTempPath);
                    }
                });
                TapFile tempFile = target.getStorage().getFile(tempPath);
                if (tempFile == null || !sameSize(sourceFile, tempFile)) {
                    throw error(FileOperationErrorCode.FILE_VERIFY_FAILED,
                            "temporary file size verification failed", tempPath);
                }
                if (!target.getStorage().move(tempPath, targetPath)) {
                    throw error(FileOperationErrorCode.FILE_WRITE_FAILED,
                            "failed to publish temporary file", targetPath);
                }
                return result(FileOperationStatus.COPIED, request, tempFile, start);
            } finally {
                try { target.getStorage().delete(tempPath); } catch (Exception ignored) { }
            }
        } catch (FileOperationException e) {
            throw e;
        } catch (Exception e) {
            throw new FileOperationException(FileOperationErrorCode.FILE_REMOTE_IO_FAILED,
                    "file operation failed", e);
        } finally {
            if (source != null) sessions.release(source);
            if (target != null) sessions.release(target);
        }
    }

    @Override
    public FileBatchResult copyBatch(List<FileCopyRequest> requests) {
        if (requests == null || requests.isEmpty()) {
            return FileBatchResult.builder().success(true).items(Collections.emptyList()).build();
        }
        if (requests.size() > MAX_BATCH_FILES) {
            throw new FileOperationException(FileOperationErrorCode.FILE_BATCH_LIMIT,
                    "file batch exceeds maximum file count");
        }
        List<FileOperationResult> items = new ArrayList<>();
        int copied = 0;
        int reused = 0;
        long bytes = 0;
        for (FileCopyRequest request : requests) {
            FileOperationResult result = copy(request);
            items.add(result);
            if (result.getStatus() == FileOperationStatus.COPIED) copied++;
            if (result.getStatus() == FileOperationStatus.REUSED) reused++;
            bytes += result.getBytes();
        }
        return FileBatchResult.builder().success(true).items(items).copied(copied).reused(reused).bytes(bytes).build();
    }

    @Override
    public FileMetadata stat(FileEndpoint endpoint, String path) {
        FileStorageSession session = sessions.retain(endpoint);
        try {
            TapFile file = session.getStorage().getFile(FilePathPolicy.resolveRoot(endpoint.getRootPath(), path));
            return file == null ? null : toMetadata(file);
        } catch (FileOperationException e) {
            throw e;
        } catch (Exception e) {
            throw new FileOperationException(FileOperationErrorCode.FILE_REMOTE_IO_FAILED, "stat failed", e);
        } finally {
            sessions.release(session);
        }
    }

    @Override
    public boolean exists(FileEndpoint endpoint, String path) {
        return stat(endpoint, path) != null;
    }

    @Override
    public List<FileMetadata> list(FileListRequest request) {
        FileStorageSession session = sessions.retain(request.getEndpoint());
        try {
            List<FileMetadata> result = new ArrayList<>();
            session.getStorage().getFilesInDirectory(
                    FilePathPolicy.resolveRoot(request.getEndpoint().getRootPath(), request.getDirectoryPath()),
                    request.getIncludeRegs(), request.getExcludeRegs(), request.isRecursive(), request.getBatchSize(),
                    files -> files.forEach(file -> result.add(toMetadata(file))));
            return result;
        } catch (Exception e) {
            throw new FileOperationException(FileOperationErrorCode.FILE_REMOTE_IO_FAILED, "list failed", e);
        } finally {
            sessions.release(session);
        }
    }

    @Override
    public FileValidationResult validate(FileEndpoint endpoint, String path, FileAccess access) {
        try {
            FilePathPolicy.resolveRoot(endpoint.getRootPath(), path);
            if (access == FileAccess.WRITE) return FileValidationResult.valid();
            return exists(endpoint, path) ? FileValidationResult.valid() :
                    FileValidationResult.invalid("path does not exist");
        } catch (Exception e) {
            return FileValidationResult.invalid(e.getMessage());
        }
    }

    @Override
    public void close() {
        sessions.close();
    }

    private FileOperationResult result(FileOperationStatus status, FileCopyRequest request, TapFile file, long start) {
        return FileOperationResult.builder().status(status).sourcePath(request.getSourcePath())
                .targetPath(request.getTargetPath()).bytes(file == null || file.getLength() == null ? 0 : file.getLength())
                .attempts(1).durationMs(System.currentTimeMillis() - start).build();
    }

    private boolean sameSize(TapFile left, TapFile right) {
        return left.getLength() != null && left.getLength().equals(right.getLength());
    }

    private FileMetadata toMetadata(TapFile file) {
        return new FileMetadata(file.getPath(), file.getLength() == null ? 0 : file.getLength(),
                file.getLastModified() == null ? 0 : file.getLastModified(), null,
                Integer.valueOf(TapFile.TYPE_DIRECTORY).equals(file.getType()));
    }

    private FileOperationException error(FileOperationErrorCode code, String message, String path) {
        return new FileOperationException(code, message, null, "copy", path);
    }
}
