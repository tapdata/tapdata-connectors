package io.tapdata.connector.csv;

import io.tapdata.connector.csv.config.CsvConfig;
import io.tapdata.file.TapFile;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CsvConnectorTest {

    @Test
    void discoverSchemaPropagatesSchemaFailureAfterCleanup() {
        RuntimeException schemaFailure = new RuntimeException("schema failed");
        IOException cleanupFailure = new IOException("destroy failed");
        TestCsvConnector connector = new TestCsvConnector("table", schemaFailure, cleanupFailure);

        RuntimeException failure = assertThrows(RuntimeException.class, () -> connector.discoverSchema(
                null, Collections.emptyList(), 0, ignored -> {
                }));

        assertSame(schemaFailure, failure);
        assertSame(cleanupFailure, failure.getSuppressed()[0]);
        assertTrue(connector.destroyCalled.get());
    }

    @Test
    void discoverSchemaPropagatesCleanupFailureWhenModelNameIsBlank() {
        IOException cleanupFailure = new IOException("destroy failed");
        TestCsvConnector connector = new TestCsvConnector("", null, cleanupFailure);

        IOException failure = assertThrows(IOException.class, () -> connector.discoverSchema(
                null, Collections.emptyList(), 0, ignored -> {
                }));

        assertSame(cleanupFailure, failure);
        assertTrue(connector.destroyCalled.get());
    }

    private static final class TestCsvConnector extends CsvConnector {
        private final String modelName;
        private final RuntimeException schemaFailure;
        private final Throwable cleanupFailure;
        private final AtomicBoolean destroyCalled = new AtomicBoolean();

        private TestCsvConnector(String modelName, RuntimeException schemaFailure, Throwable cleanupFailure) {
            this.modelName = modelName;
            this.schemaFailure = schemaFailure;
            this.cleanupFailure = cleanupFailure;
        }

        @Override
        protected void initConnection(TapConnectionContext connectorContext) {
            fileConfig = new CsvConfig();
            fileConfig.setModelName(modelName);
        }

        @Override
        protected ConcurrentMap<String, TapFile> getFilteredFiles() throws Exception {
            if (schemaFailure != null) {
                throw schemaFailure;
            }
            return new ConcurrentHashMap<>();
        }

        @Override
        protected Throwable destroyStorage(Throwable failure) {
            destroyCalled.set(true);
            if (cleanupFailure == null) {
                return failure;
            }
            if (failure == null) {
                return cleanupFailure;
            }
            failure.addSuppressed(cleanupFailure);
            return failure;
        }
    }
}
