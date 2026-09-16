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

import static org.junit.jupiter.api.Assertions.assertThrows;
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

    private static final class NoopStorage implements TapFileStorage {
        @Override
        public void init(Map<String, Object> params) {
        }

        @Override
        public void destroy() {
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
