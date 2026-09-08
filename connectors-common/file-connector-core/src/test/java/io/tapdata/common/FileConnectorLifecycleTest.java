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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileConnectorLifecycleTest {

    @Test
    void onStopDestroysStorageWhenMergeFails() throws Exception {
        TrackingStorage storage = new TrackingStorage(false);
        TrackingWriter writer = new TrackingWriter(storage, true, false);
        TestFileConnector connector = new TestFileConnector(storage, writer);

        assertThrows(RuntimeException.class, () -> connector.onStop(null));

        assertTrue(writer.releaseCalled);
        assertTrue(storage.destroyCalled);
    }

    @Test
    void onStopDestroysStorageWhenWriterReleaseFails() throws Exception {
        TrackingStorage storage = new TrackingStorage(true);
        TrackingWriter writer = new TrackingWriter(storage, false, true);
        TestFileConnector connector = new TestFileConnector(storage, writer);

        assertThrows(RuntimeException.class, () -> connector.onStop(null));

        assertTrue(storage.destroyCalled);
    }

    private static final class TestFileConnector extends FileConnector {
        private TestFileConnector(TrackingStorage storage, TrackingWriter writer) {
            this.storage = storage;
            this.fileRecordWriter = writer;
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
        private boolean releaseCalled;

        private TrackingWriter(TapFileStorage storage, boolean mergeFails, boolean releaseFails) throws Exception {
            super(storage, new FileConfig(), new TapTable("table").add(new TapField("id", "STRING")), new EmptyKvMap());
            this.mergeFails = mergeFails;
            this.releaseFails = releaseFails;
        }

        @Override
        public void mergeCacheFiles() {
            if (mergeFails) {
                throw new RuntimeException("merge failed");
            }
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
        public void destroy() {
            destroyCalled = true;
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
