package io.tapdata.common.file;

import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.file.operation.FileCopyRequest;
import io.tapdata.file.operation.FileEndpoint;
import io.tapdata.file.operation.FileOperationErrorCode;
import io.tapdata.file.operation.FileOperationException;
import io.tapdata.file.operation.FileOperationStatus;
import io.tapdata.file.operation.FileStorageCapability;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

class DefaultFileOperationServiceTest {

    @Test
    void copiesThenPublishesAndReusesMatchingTarget() {
        FakeStorage source = new FakeStorage();
        source.put("/orders/a.txt", "hello".getBytes());
        FakeStorage target = new FakeStorage();
        InMemorySessions sessions = new InMemorySessions(source, target);
        DefaultFileOperationService service = new DefaultFileOperationService(sessions);
        FileCopyRequest request = request();

        assertEquals(FileOperationStatus.COPIED, service.copy(request).getStatus());
        assertArrayEquals("hello".getBytes(), target.bytes("/orders/a.txt"));
        assertEquals(FileOperationStatus.REUSED, service.copy(request).getStatus());
        assertEquals(1, target.publishCount);
    }

    @Test
    void rejectsExistingDifferentTargetWithoutOverwrite() {
        FakeStorage source = new FakeStorage();
        source.put("/orders/a.txt", "hello".getBytes());
        FakeStorage target = new FakeStorage();
        target.put("/orders/a.txt", "different".getBytes());
        DefaultFileOperationService service = new DefaultFileOperationService(
                new InMemorySessions(source, target));

        FileOperationException error = assertThrows(FileOperationException.class,
                () -> service.copy(request()));
        assertEquals(FileOperationErrorCode.FILE_TARGET_CONFLICT, error.getCode());
    }

    @Test
    void dryRunDoesNotWriteTarget() {
        FakeStorage source = new FakeStorage();
        source.put("/orders/a.txt", "hello".getBytes());
        FakeStorage target = new FakeStorage();
        DefaultFileOperationService service = new DefaultFileOperationService(
                new InMemorySessions(source, target));

        FileOperationStatus status = service.copy(FileCopyRequest.builder()
                .source(request().getSource())
                .target(request().getTarget())
                .sourcePath("orders/a.txt")
                .targetPath("orders/a.txt")
                .dryRun(true)
                .build()).getStatus();
        assertEquals(FileOperationStatus.DRY_RUN, status);
        assertNull(target.getFile("/orders/a.txt"));
    }

    @Test
    void checksumVerificationRejectsSameSizeDifferentContent() {
        FakeStorage source = new FakeStorage();
        source.put("/orders/a.txt", "hello".getBytes());
        FakeStorage target = new FakeStorage();
        target.put("/orders/a.txt", "world".getBytes());
        DefaultFileOperationService service = new DefaultFileOperationService(
                new InMemorySessions(source, target));

        FileOperationException error = assertThrows(FileOperationException.class,
                () -> service.copy(FileCopyRequest.builder()
                        .source(request().getSource()).target(request().getTarget())
                        .sourcePath("orders/a.txt").targetPath("orders/a.txt")
                        .verifyMode(io.tapdata.file.operation.FileVerifyMode.CHECKSUM).build()));
        assertEquals(FileOperationErrorCode.FILE_TARGET_CONFLICT, error.getCode());
    }

    @Test
    void checksumVerificationAndRetryAreReported() {
        FakeStorage source = new FakeStorage();
        source.put("/orders/a.txt", "hello".getBytes());
        FakeStorage target = new FakeStorage();
        target.failSaveCount = 1;
        DefaultFileOperationService service = new DefaultFileOperationService(
                new InMemorySessions(source, target));

        io.tapdata.file.operation.FileOperationResult result = service.copy(FileCopyRequest.builder()
                .source(request().getSource()).target(request().getTarget())
                .sourcePath("orders/a.txt").targetPath("orders/a.txt")
                .verifyMode(io.tapdata.file.operation.FileVerifyMode.CHECKSUM)
                .expectedChecksum("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
                .retryTimes(1).build());
        assertEquals(FileOperationStatus.COPIED, result.getStatus());
        assertEquals(2, result.getAttempts());
        assertEquals("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
                result.getChecksum());
    }

    private static FileCopyRequest request() {
        Map<String, Object> sourceParams = new HashMap<>();
        sourceParams.put("side", "source");
        Map<String, Object> targetParams = new HashMap<>();
        targetParams.put("side", "target");
        FileEndpoint source = FileEndpoint.builder().protocol("fake").params(sourceParams).rootPath("/").build();
        FileEndpoint target = FileEndpoint.builder().protocol("fake").params(targetParams).rootPath("/").build();
        return FileCopyRequest.builder().source(source).target(target)
                .sourcePath("orders/a.txt").targetPath("orders/a.txt").build();
    }

    private static class InMemorySessions implements FileStorageSessionManager {
        private final FakeStorage source;
        private final FakeStorage target;
        InMemorySessions(FakeStorage source, FakeStorage target) {
            this.source = source;
            this.target = target;
        }
        @Override
        public FileStorageSession retain(FileEndpoint endpoint) {
            TapFileStorage storage = "target".equals(endpoint.getParams().get("side")) ? target : source;
            return new FileStorageSession(endpoint, "fake", storage);
        }
        @Override public void release(FileStorageSession session) { }
        @Override public void invalidate(FileStorageSession session) { }
        @Override public void close() { }
    }

    private static class FakeStorage implements TapFileStorage {
        private final Map<String, byte[]> files = new HashMap<>();
        private int publishCount;
        private int failSaveCount;

        void put(String path, byte[] bytes) { files.put(path, bytes); }
        byte[] bytes(String path) { return files.get(path); }
        @Override public void init(Map<String, Object> params) { }
        @Override public void destroy() { }
        @Override public TapFile getFile(String path) {
            byte[] bytes = files.get(path);
            return bytes == null ? null : new TapFile().type(TapFile.TYPE_FILE).path(path).length((long) bytes.length);
        }
        @Override public void readFile(String path, Consumer<InputStream> consumer) throws Exception {
            byte[] bytes = files.get(path);
            if (bytes != null) consumer.accept(new ByteArrayInputStream(bytes));
        }
        @Override public InputStream readFile(String path) {
            byte[] bytes = files.get(path);
            return bytes == null ? null : new ByteArrayInputStream(bytes);
        }
        @Override public boolean isFileExist(String path) { return files.containsKey(path); }
        @Override public boolean move(String sourcePath, String destPath) {
            byte[] value = files.remove(sourcePath);
            if (value == null) return false;
            files.put(destPath, value);
            publishCount++;
            return true;
        }
        @Override public boolean delete(String path) { return files.remove(path) != null; }
        @Override public TapFile saveFile(String path, InputStream is, boolean canReplace) throws Exception {
            if (failSaveCount > 0) {
                failSaveCount--;
                throw new java.io.IOException("temporary write failure");
            }
            if (!canReplace && files.containsKey(path)) return getFile(path);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            int read;
            while ((read = is.read(buffer)) >= 0) out.write(buffer, 0, read);
            files.put(path, out.toByteArray());
            return getFile(path);
        }
        @Override public OutputStream openFileOutputStream(String path, boolean append) { return new ByteArrayOutputStream(); }
        @Override public void getFilesInDirectory(String directoryPath, Collection<String> includeRegs,
                                                  Collection<String> excludeRegs, boolean recursive, int batchSize,
                                                  Consumer<List<TapFile>> consumer) { }
        @Override public boolean isDirectoryExist(String path) { return true; }
        @Override public String getConnectInfo() { return "fake://"; }
        @Override public EnumSet<FileStorageCapability> capabilities() {
            return EnumSet.of(FileStorageCapability.ATOMIC_RENAME, FileStorageCapability.MAKE_DIRECTORY);
        }
    }
}
