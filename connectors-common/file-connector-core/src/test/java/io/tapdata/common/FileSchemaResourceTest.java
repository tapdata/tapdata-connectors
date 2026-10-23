package io.tapdata.common;

import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileSchemaResourceTest {

    @Test
    void sampleEveryFileDataPreservesInterruptStatus() {
        FileSchema schema = new FileSchema(new FileConfig(), new NoopStorage()) {
            @Override
            protected void sampleOneFile(Map<String, Object> sampleResult, TapFile tapFile) {
            }
        };

        Thread.currentThread().interrupt();
        try {
            assertThrows(RuntimeException.class, () -> schema.sampleEveryFileData(new ConcurrentHashMap<>()));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void interruptedSamplingClosesStorageAndReturnsAfterWorkerShutdownTimeout() throws Exception {
        CountDownLatch sampleStarted = new CountDownLatch(1);
        CountDownLatch workerInterrupted = new CountDownLatch(1);
        CountDownLatch allowWorkerExit = new CountDownLatch(1);
        CountDownLatch samplingFinished = new CountDownLatch(1);
        AtomicReference<Throwable> samplingFailure = new AtomicReference<>();
        NoopStorage storage = new NoopStorage();
        FileSchema schema = new FileSchema(new FileConfig(), storage) {
            @Override
            protected void sampleOneFile(Map<String, Object> sampleResult, TapFile tapFile) throws Exception {
                sampleStarted.countDown();
                try {
                    new CountDownLatch(1).await();
                } catch (InterruptedException e) {
                    workerInterrupted.countDown();
                    boolean workerReleased = false;
                    while (!workerReleased) {
                        try {
                            workerReleased = allowWorkerExit.await(5, TimeUnit.SECONDS);
                        } catch (InterruptedException ignored) {
                            // Simulate an I/O operation that ignores repeated interrupts until it finishes.
                        }
                    }
                }
            }
        };
        ConcurrentHashMap<String, TapFile> fileMap = new ConcurrentHashMap<>();
        fileMap.put("/file.txt", new TapFile());
        Thread sampler = new Thread(() -> {
            try {
                schema.sampleEveryFileData(fileMap);
            } catch (Throwable throwable) {
                samplingFailure.set(throwable);
            } finally {
                samplingFinished.countDown();
            }
        });
        sampler.setDaemon(true);
        sampler.start();

        assertTrue(sampleStarted.await(1, TimeUnit.SECONDS));
        sampler.interrupt();
        assertTrue(workerInterrupted.await(1, TimeUnit.SECONDS));
        assertTrue(storage.destroyCalled.await(1, TimeUnit.SECONDS));
        try {
            assertTrue(samplingFinished.await(11, TimeUnit.SECONDS));
        } finally {
            allowWorkerExit.countDown();
        }

        sampler.join(1000);

        assertFalse(sampler.isAlive());
        assertTrue(samplingFailure.get() instanceof RuntimeException);
        assertNull(fileMap.get("/file.txt"));
    }

    private static final class NoopStorage implements TapFileStorage {
        private final CountDownLatch destroyCalled = new CountDownLatch(1);

        @Override
        public void init(Map<String, Object> params) {
        }

        @Override
        public void destroy() {
            destroyCalled.countDown();
        }

        @Override
        public TapFile getFile(String path) {
            return null;
        }

        @Override
        public void readFile(String path, java.util.function.Consumer<InputStream> consumer) {
        }

        @Override
        public InputStream readFile(String path) {
            return null;
        }

        @Override
        public boolean isFileExist(String path) {
            return false;
        }

        @Override
        public boolean move(String sourcePath, String destPath) {
            return false;
        }

        @Override
        public boolean delete(String path) {
            return false;
        }

        @Override
        public TapFile saveFile(String path, InputStream is, boolean canReplace) {
            return null;
        }

        @Override
        public OutputStream openFileOutputStream(String path, boolean append) {
            return null;
        }

        @Override
        public boolean supportAppendData() {
            return false;
        }

        @Override
        public void getFilesInDirectory(String directoryPath,
                                        Collection<String> includeRegs,
                                        Collection<String> excludeRegs,
                                        boolean recursive,
                                        int batchSize,
                                        java.util.function.Consumer<List<TapFile>> consumer) {
        }

        @Override
        public boolean isDirectoryExist(String path) {
            return false;
        }

        @Override
        public String getConnectInfo() {
            return "test";
        }
    }
}
