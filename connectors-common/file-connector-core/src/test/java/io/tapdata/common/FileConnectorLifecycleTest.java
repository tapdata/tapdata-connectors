package io.tapdata.common;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileConnectorLifecycleTest {

    @Test
    void onStopDestroysStorageWhenMergeFails() throws Exception {
        TrackingStorage storage = new TrackingStorage(false);
        TrackingWriter writer = new TrackingWriter(storage, true, false);
        TestFileConnector connector = new TestFileConnector(storage, writer);

        RuntimeException failure = assertThrows(RuntimeException.class, () -> connector.onStop(null));

        assertTrue(writer.releaseCalled);
        assertTrue(storage.destroyCalled);
        assertEquals("merge failed", failure.getMessage());
    }

    @Test
    void onStopDestroysStorageWhenWriterReleaseFails() throws Exception {
        TrackingStorage storage = new TrackingStorage(true);
        TrackingWriter writer = new TrackingWriter(storage, false, true);
        TestFileConnector connector = new TestFileConnector(storage, writer);

        assertThrows(RuntimeException.class, () -> connector.onStop(null));

        assertTrue(storage.destroyCalled);
    }

    @Test
    void destroyStoragePreservesPrimaryFailureWhenCleanupFails() throws Exception {
        TrackingStorage storage = new TrackingStorage(false);
        IOException cleanupFailure = new IOException("destroy failed");
        storage.onDestroyFailure(cleanupFailure);
        TestFileConnector connector = new TestFileConnector(storage, new TrackingWriter(storage, false, false));
        RuntimeException primaryFailure = new RuntimeException("schema failed");

        Throwable failure = connector.destroyStorageForTest(primaryFailure);

        assertSame(primaryFailure, failure);
        assertArrayEquals(new Throwable[]{cleanupFailure}, primaryFailure.getSuppressed());
    }

    @Test
    void destroyStorageReturnsCleanupFailureWhenNoPrimaryFailure() throws Exception {
        TrackingStorage storage = new TrackingStorage(false);
        IOException cleanupFailure = new IOException("destroy failed");
        storage.onDestroyFailure(cleanupFailure);
        TestFileConnector connector = new TestFileConnector(storage, new TrackingWriter(storage, false, false));

        Throwable failure = connector.destroyStorageForTest(null);

        assertSame(cleanupFailure, failure);
    }

    @Test
    void onStopSerializesWithActiveMergeBeforeReleasingResources() throws Exception {
        TrackingStorage storage = new TrackingStorage(false);
        TrackingWriter writer = new TrackingWriter(storage, false, false);
        TestFileConnector connector = new TestFileConnector(storage, writer);
        CountDownLatch mergeStarted = new CountDownLatch(1);
        CountDownLatch allowMerge = new CountDownLatch(1);
        CountDownLatch concurrentMergeDetected = new CountDownLatch(1);
        writer.blockMerge(mergeStarted, allowMerge, concurrentMergeDetected);

        AtomicReference<Throwable> backgroundFailure = new AtomicReference<>();
        Thread backgroundMerge = new Thread(() -> {
            try {
                connector.mergeCacheFilesForTest();
            } catch (Throwable throwable) {
                backgroundFailure.set(throwable);
            }
        });
        backgroundMerge.start();
        assertTrue(mergeStarted.await(1, TimeUnit.SECONDS));

        AtomicReference<Throwable> stopFailure = new AtomicReference<>();
        Thread stopper = new Thread(() -> {
            try {
                connector.onStop(null);
            } catch (Throwable throwable) {
                stopFailure.set(throwable);
            }
        });
        stopper.start();

        assertFalse(concurrentMergeDetected.await(200, TimeUnit.MILLISECONDS));
        allowMerge.countDown();

        backgroundMerge.join(1000);
        stopper.join(1000);

        assertFalse(backgroundMerge.isAlive());
        assertFalse(stopper.isAlive());
        assertNull(backgroundFailure.get());
        assertNull(stopFailure.get());
        assertEquals(1, writer.maxConcurrentMerges());
        assertTrue(storage.destroyCalled);
    }

    @Test
    void onStopForceDestroysStorageWhenMergeIgnoresInterrupt() throws Exception {
        TrackingStorage storage = new TrackingStorage(false);
        TrackingWriter writer = new TrackingWriter(storage, false, false);
        TestFileConnector connector = new TestFileConnector(storage, writer);
        CountDownLatch mergeStarted = new CountDownLatch(1);
        CountDownLatch allowMerge = new CountDownLatch(1);
        writer.blockMergeIgnoringInterrupt(mergeStarted, allowMerge);
        storage.onDestroy(allowMerge::countDown);
        connector.setExecutorService(new TrackingStorage.NeverTerminatingExecutor());

        AtomicReference<Throwable> backgroundFailure = new AtomicReference<>();
        Thread backgroundMerge = new Thread(() -> {
            try {
                connector.mergeCacheFilesForTest();
            } catch (Throwable throwable) {
                backgroundFailure.set(throwable);
            }
        });
        backgroundMerge.setDaemon(true);
        backgroundMerge.start();
        assertTrue(mergeStarted.await(1, TimeUnit.SECONDS));

        AtomicReference<Throwable> stopFailure = new AtomicReference<>();
        Thread stopper = new Thread(() -> {
            try {
                connector.onStop(null);
            } catch (Throwable throwable) {
                stopFailure.set(throwable);
            }
        });
        stopper.setDaemon(true);
        stopper.start();

        try {
            stopper.join(2000);
            assertFalse(stopper.isAlive());
            assertTrue(storage.destroyCalled);
            assertTrue(writer.releaseCalled);
            assertNull(stopFailure.get());
        } finally {
            allowMerge.countDown();
            backgroundMerge.join(1000);
            stopper.join(1000);
        }
        assertFalse(backgroundMerge.isAlive());
        assertNull(backgroundFailure.get());
    }

    private static final class TestFileConnector extends FileConnector {
        private TestFileConnector(TrackingStorage storage, TrackingWriter writer) {
            this.storage = storage;
            this.fileRecordWriter = writer;
        }

        private void mergeCacheFilesForTest() throws Exception {
            mergeCacheFilesSafely();
        }

        private void setExecutorService(ExecutorService executorService) {
            this.executorService = executorService;
        }

        private Throwable destroyStorageForTest(Throwable failure) {
            return destroyStorage(failure);
        }

        @Override
        protected void readOneFile(FileOffset fileOffset,
                                   TapTable tapTable,
                                   int eventBatchSize,
                                   BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer,
                                   AtomicReference<List<TapEvent>> tapEvents) {
        }

        @Override
        public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        }

        @Override
        public void discoverSchema(TapConnectionContext connectionContext,
                                   List<String> tables,
                                   int tableSize,
                                   Consumer<List<TapTable>> consumer) {
        }
    }

    private static final class TrackingWriter extends AbstractFileRecordWriter {
        private final boolean mergeFails;
        private final boolean releaseFails;
        private final AtomicInteger activeMerges = new AtomicInteger();
        private final AtomicInteger maxConcurrentMerges = new AtomicInteger();
        private boolean releaseCalled;
        private CountDownLatch mergeStarted;
        private CountDownLatch allowMerge;
        private CountDownLatch concurrentMergeDetected;
        private boolean ignoreInterrupt;

        private TrackingWriter(TapFileStorage storage, boolean mergeFails, boolean releaseFails) throws Exception {
            super(storage, new FileConfig(), new TapTable("table").add(new TapField("id", "STRING")), new EmptyKvMap());
            this.mergeFails = mergeFails;
            this.releaseFails = releaseFails;
        }

        @Override
        public void mergeCacheFiles() {
            int currentMerges = activeMerges.incrementAndGet();
            maxConcurrentMerges.updateAndGet(current -> Math.max(current, currentMerges));
            if (currentMerges > 1 && concurrentMergeDetected != null) {
                concurrentMergeDetected.countDown();
            }
            try {
                if (mergeStarted != null) {
                    mergeStarted.countDown();
                    if (ignoreInterrupt) {
                        boolean interrupted = false;
                        while (true) {
                            try {
                                allowMerge.await();
                                break;
                            } catch (InterruptedException e) {
                                interrupted = true;
                            }
                        }
                        if (interrupted) {
                            Thread.currentThread().interrupt();
                        }
                    } else if (!allowMerge.await(1, TimeUnit.SECONDS)) {
                        throw new RuntimeException("merge was not released");
                    }
                }
                if (mergeFails) {
                    throw new RuntimeException("merge failed");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } finally {
                activeMerges.decrementAndGet();
            }
        }

        private void blockMerge(CountDownLatch mergeStarted,
                                CountDownLatch allowMerge,
                                CountDownLatch concurrentMergeDetected) {
            this.mergeStarted = mergeStarted;
            this.allowMerge = allowMerge;
            this.concurrentMergeDetected = concurrentMergeDetected;
        }

        private void blockMergeIgnoringInterrupt(CountDownLatch mergeStarted,
                                                  CountDownLatch allowMerge) {
            this.mergeStarted = mergeStarted;
            this.allowMerge = allowMerge;
            this.ignoreInterrupt = true;
        }

        private int maxConcurrentMerges() {
            return maxConcurrentMerges.get();
        }

        @Override
        public void releaseResource() {
            releaseCalled = true;
            if (releaseFails) {
                throw new RuntimeException("release failed");
            }
        }

        @Override
        protected void writeOneFile(List<TapRecordEvent> tapRecordEvents,
                                    Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer,
                                    String uniquePath) {
        }

        @Override
        protected void writeMultiFiles(List<TapRecordEvent> tapRecordEvents,
                                       Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer,
                                       String fileNameExpression) {
        }

        @Override
        public void write(List<TapRecordEvent> tapRecordEvents,
                          Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) {
        }

        @Override
        protected void writeCacheFile(String coreLocalFilePath, List<TapFile> cacheFilesPath) {
        }
    }

    private static final class TrackingStorage implements TapFileStorage {
        private final boolean appendSupported;
        private boolean destroyCalled;

        private TrackingStorage(boolean appendSupported) {
            this.appendSupported = appendSupported;
        }

        @Override
        public void init(Map<String, Object> params) {
        }

        @Override
        public void destroy() throws IOException {
            destroyCalled = true;
            if (destroyAction != null) {
                destroyAction.run();
            }
            if (destroyFailure != null) {
                throw destroyFailure;
            }
        }

        @Override
        public TapFile getFile(String path) {
            return null;
        }

        @Override
        public void readFile(String path, Consumer<InputStream> consumer) {
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
            return appendSupported;
        }

        private void onDestroy(Runnable action) {
            this.destroyAction = action;
        }

        private void onDestroyFailure(IOException failure) {
            this.destroyFailure = failure;
        }

        @Override
        public void getFilesInDirectory(String directoryPath,
                                        Collection<String> includeRegs,
                                        Collection<String> excludeRegs,
                                        boolean recursive,
                                        int batchSize,
                                        Consumer<List<TapFile>> consumer) {
        }

        @Override
        public boolean isDirectoryExist(String path) {
            return false;
        }

        @Override
        public String getConnectInfo() {
            return "test";
        }

        private Runnable destroyAction;
        private IOException destroyFailure;

        private static final class NeverTerminatingExecutor extends AbstractExecutorService {
            @Override
            public void shutdown() {
            }

            @Override
            public List<Runnable> shutdownNow() {
                return Collections.emptyList();
            }

            @Override
            public boolean isShutdown() {
                return true;
            }

            @Override
            public boolean isTerminated() {
                return false;
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) {
                return false;
            }

            @Override
            public void execute(Runnable command) {
            }
        }
    }

    private static final class EmptyKvMap implements KVMap<Object> {
        @Override
        public Object get(String key) {
            return null;
        }

        @Override
        public void init(String mapKey, Class<Object> valueClass) {
        }

        @Override
        public void put(String key, Object value) {
        }

        @Override
        public Object putIfAbsent(String key, Object value) {
            return null;
        }

        @Override
        public Object remove(String key) {
            return null;
        }

        @Override
        public void clear() {
        }

        @Override
        public void reset() {
        }
    }
}
