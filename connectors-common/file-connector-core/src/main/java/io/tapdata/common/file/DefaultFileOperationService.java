package io.tapdata.common.file;

import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.file.operation.*;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultFileOperationService implements TapFileOperationService {
    private static final int MAX_BATCH_FILES = 100;
    private static final int BUFFER_SIZE = 8192;
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
        FileOperationException failure = null;
        int maxAttempts = request.getRetryTimes() + 1;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                FileOperationResult result = copyOnce(request, start);
                return withAttempts(result, attempt);
            } catch (FileOperationException e) {
                failure = e;
                if (!retryable(e.getCode()) || attempt == maxAttempts || timedOut(start, request.getTimeoutMs())) {
                    throw e;
                }
                try {
                    Thread.sleep(Math.min(50L * attempt, 250L));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new FileOperationException(FileOperationErrorCode.FILE_TIMEOUT,
                            "file operation interrupted", interrupted, "retry", request.getTargetPath());
                }
            }
        }
        throw failure;
    }

    private FileOperationResult copyOnce(FileCopyRequest request, long start) {
        FileStorageSession source = null;
        FileStorageSession target = null;
        try {
            checkTimeout(start, request.getTimeoutMs(), request.getTargetPath());
            source = sessions.retain(request.getSource());
            target = sessions.retain(request.getTarget());
            String sourcePath = FilePathPolicy.resolveRoot(request.getSource().getRootPath(), request.getSourcePath());
            String targetPath = FilePathPolicy.resolveRoot(request.getTarget().getRootPath(), request.getTargetPath());
            TapFile sourceFile = source.getStorage().getFile(sourcePath);
            if (sourceFile == null || !Integer.valueOf(TapFile.TYPE_FILE).equals(sourceFile.getType())) {
                throw error(FileOperationErrorCode.FILE_NOT_FOUND, "source file does not exist", sourcePath);
            }
            String sourceChecksum = needsChecksum(request)
                    ? checksum(source.getStorage(), sourcePath, start, request.getTimeoutMs()) : null;
            verifyExpectedChecksum(request, sourceChecksum, sourcePath);
            if (request.isDryRun()) {
                return result(FileOperationStatus.DRY_RUN, request, sourceFile, sourceChecksum, start);
            }

            TapFile existing = target.getStorage().getFile(targetPath);
            if (existing != null && !request.isOverwrite()) {
                if (reusable(request, source, target, sourcePath, targetPath, sourceFile, existing,
                        sourceChecksum, start)) {
                    return result(FileOperationStatus.REUSED, request, existing, sourceChecksum, start);
                }
                throw error(FileOperationErrorCode.FILE_TARGET_CONFLICT,
                        "target file already exists with different content", targetPath);
            }

            EnumSet<FileStorageCapability> capabilities = target.getStorage().capabilities();
            if (!capabilities.contains(FileStorageCapability.ATOMIC_RENAME)) {
                throw error(FileOperationErrorCode.FILE_UNSUPPORTED_OPERATION,
                        "target storage does not support atomic rename", targetPath);
            }
            ensureParentDirectory(target.getStorage(), targetPath, capabilities);
            String tempRelative = ".tapdata-tmp/" + Thread.currentThread().getId() + "/" +
                    Math.abs(System.nanoTime()) + ".part";
            String tempPath = FilePathPolicy.resolveRoot(request.getTarget().getRootPath(), tempRelative);
            ensureParentDirectory(target.getStorage(), tempPath, capabilities);
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
                checkTimeout(start, request.getTimeoutMs(), tempPath);
                TapFile tempFile = target.getStorage().getFile(tempPath);
                if (tempFile == null || !sameSize(sourceFile, tempFile)) {
                    throw error(FileOperationErrorCode.FILE_VERIFY_FAILED,
                            "temporary file size verification failed", tempPath);
                }
                String tempChecksum = needsChecksum(request)
                        ? checksum(target.getStorage(), tempPath, start, request.getTimeoutMs()) : null;
                verifyExpectedChecksum(request, tempChecksum, tempPath);
                if (!target.getStorage().move(tempPath, targetPath)) {
                    throw error(FileOperationErrorCode.FILE_WRITE_FAILED,
                            "failed to publish temporary file", targetPath);
                }
                checkTimeout(start, request.getTimeoutMs(), targetPath);
                return result(FileOperationStatus.COPIED, request, tempFile, tempChecksum, start);
            } finally {
                try {
                    target.getStorage().delete(tempPath);
                } catch (Exception ignored) {
                }
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

    private boolean reusable(FileCopyRequest request, FileStorageSession source, FileStorageSession target,
                             String sourcePath, String targetPath, TapFile sourceFile, TapFile existing,
                             String sourceChecksum, long start) {
        if (request.getVerifyMode() == FileVerifyMode.NONE) return false;
        if (request.getVerifyMode() == FileVerifyMode.SIZE) return sameSize(sourceFile, existing);
        String targetChecksum = checksum(target.getStorage(), targetPath, start, request.getTimeoutMs());
        return sourceChecksum != null && sourceChecksum.equalsIgnoreCase(targetChecksum);
    }

    private boolean needsChecksum(FileCopyRequest request) {
        return request.getVerifyMode() == FileVerifyMode.CHECKSUM || request.getExpectedChecksum() != null;
    }

    private void verifyExpectedChecksum(FileCopyRequest request, String actual, String path) {
        if (request.getExpectedChecksum() != null &&
                (actual == null || !request.getExpectedChecksum().equalsIgnoreCase(actual))) {
            throw error(FileOperationErrorCode.FILE_VERIFY_FAILED, "checksum verification failed", path);
        }
    }

    private String checksum(TapFileStorage storage, String path, long start, long timeoutMs) {
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new FileOperationException(FileOperationErrorCode.FILE_VERIFY_FAILED,
                    "SHA-256 is unavailable", e, "verify", path);
        }
        AtomicReference<IOException> readError = new AtomicReference<>();
        try {
            storage.readFile(path, input -> {
                if (input == null) return;
                byte[] buffer = new byte[BUFFER_SIZE];
                try (InputStream stream = input) {
                    int read;
                    while ((read = stream.read(buffer)) != -1) {
                        digest.update(buffer, 0, read);
                        checkTimeout(start, timeoutMs, path);
                    }
                } catch (IOException e) {
                    readError.set(e);
                }
            });
        } catch (Exception e) {
            throw new FileOperationException(FileOperationErrorCode.FILE_REMOTE_IO_FAILED,
                    "failed to read file for checksum", e, "verify", path);
        }
        if (readError.get() != null) {
            throw new FileOperationException(FileOperationErrorCode.FILE_REMOTE_IO_FAILED,
                    "failed to read file for checksum", readError.get(), "verify", path);
        }
        return hex(digest.digest());
    }

    private String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) result.append(String.format("%02x", value & 0xff));
        return result.toString();
    }

    private void ensureParentDirectory(TapFileStorage storage, String path,
                                       EnumSet<FileStorageCapability> capabilities) throws Exception {
        int slash = path.lastIndexOf('/');
        if (slash <= 0) return;
        String parent = path.substring(0, slash);
        if (storage.isDirectoryExist(parent)) return;
        if (!capabilities.contains(FileStorageCapability.MAKE_DIRECTORY)) {
            throw error(FileOperationErrorCode.FILE_UNSUPPORTED_OPERATION,
                    "target storage does not support directory creation", parent);
        }
        String[] segments = parent.split("/");
        String current = "";
        for (String segment : segments) {
            if (segment.isEmpty()) continue;
            current += "/" + segment;
            if (!storage.isDirectoryExist(current) && !storage.makeDirectory(current)) {
                throw error(FileOperationErrorCode.FILE_WRITE_FAILED,
                        "failed to create target directory", current);
            }
        }
    }

    private void checkTimeout(long start, long timeoutMs, String path) {
        if (timedOut(start, timeoutMs)) {
            throw error(FileOperationErrorCode.FILE_TIMEOUT, "file operation timed out", path);
        }
    }

    private boolean timedOut(long start, long timeoutMs) {
        return timeoutMs > 0 && System.currentTimeMillis() - start > timeoutMs;
    }

    private boolean retryable(FileOperationErrorCode code) {
        return code == FileOperationErrorCode.FILE_REMOTE_IO_FAILED ||
                code == FileOperationErrorCode.FILE_CONNECT_FAILED ||
                code == FileOperationErrorCode.FILE_WRITE_FAILED;
    }

    private FileOperationResult withAttempts(FileOperationResult result, int attempts) {
        return FileOperationResult.builder().status(result.getStatus()).sourcePath(result.getSourcePath())
                .targetPath(result.getTargetPath()).bytes(result.getBytes()).checksum(result.getChecksum())
                .attempts(attempts).durationMs(result.getDurationMs()).build();
    }

    private FileOperationResult result(FileOperationStatus status, FileCopyRequest request, TapFile file,
                                       String checksum, long start) {
        return FileOperationResult.builder().status(status).sourcePath(request.getSourcePath())
                .targetPath(request.getTargetPath()).bytes(file == null || file.getLength() == null ? 0 : file.getLength())
                .checksum(checksum).attempts(1).durationMs(System.currentTimeMillis() - start).build();
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
