package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableWrite;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonServiceWriteErrorLogTest {

    private static final String DATABASE = "test_db";
    private static final String TABLE_NAME = "customer_table";
    private static final String CACHE_KEY = DATABASE + "." + TABLE_NAME;
    private static final long REFERENCE_TIME = 123456789L;
    private static final String LONG_NAME = "x".repeat(300);

    @ParameterizedTest(name = "{0}")
    @MethodSource("writeFailureScenarios")
    void shouldLogFriendlyFailureDetailsForCdcScenarios(Scenario scenario) throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase(DATABASE);
        config.setBucketMode("fixed");
        Map<String, DataMap> tableConfig = new LinkedHashMap<>();
        tableConfig.put(TABLE_NAME, DataMap.create().kv("bucketMode", "fixed"));
        config.setTableConfig(tableConfig);

        CapturingLog log = new CapturingLog();
        PaimonService service = new PaimonService(config, log);

        TapTable table = mock(TapTable.class);
        when(table.getName()).thenReturn(TABLE_NAME);

        seedComputeHashKey(service);
        seedFieldCache(service, List.of(
                new DataField(0, "id", DataTypes.BIGINT()),
                new DataField(1, "name", DataTypes.STRING())
        ));

        try (FailingStreamTableWrite writer = new FailingStreamTableWrite(scenario.failOnWriteCall())) {
            Exception thrown = assertThrows(Exception.class, () -> scenario.invoke(service, writer, table));
            assertEquals(scenario.expectedExceptionMessage(), thrown.getMessage());

            assertNotNull(writer.getFailedRow());
            assertEquals(scenario.expectedRowKind(), writer.getFailedRow().getRowKind().name());
            assertNull(writer.getFailedRow().getField(0));
            assertEquals(scenario.expectedSecondFieldValue(), writer.getFailedRow().getField(1).toString());
            assertEquals(scenario.expectedWriteCount(), writer.getWriteCount());
        }

        String errorMessage = log.getLastErrorMessage();
        assertNotNull(errorMessage);
        System.out.println(errorMessage);
        assertLogContains(errorMessage, "Failed to write row to Paimon.");
        assertLogContains(errorMessage, "table=customer_table");
        assertLogContains(errorMessage, "bucket=null");
        assertLogContains(errorMessage, scenario.expectedSourceDataFragment());
        assertLogContains(errorMessage, "event=" + scenario.expectedEventClassFragment());
        assertLogContains(errorMessage, scenario.expectedEventDetailFragment());
        assertLogContains(errorMessage, "row=GenericRow{");
        assertLogContains(errorMessage, "fieldMapping=[0:id, 1:name]");
        assertLogContains(errorMessage, "rowKind=" + scenario.expectedRowKind());
        assertLogContains(errorMessage, scenario.expectedRowValuesFragment());
        if (scenario.expectedTruncationFragment() != null) {
            assertLogContains(errorMessage, scenario.expectedTruncationFragment());
        }
    }

    private static Stream<Arguments> writeFailureScenarios() {
        return Stream.of(
                Arguments.of(Scenario.insertNullRequiredField()),
                Arguments.of(Scenario.updateBeforeNullRequiredField()),
                Arguments.of(Scenario.updateAfterNullRequiredField()),
                Arguments.of(Scenario.insertNullRequiredFieldWithLongValue()),
                Arguments.of(Scenario.deleteMissingRequiredField())
        );
    }

    private static void assertLogContains(String errorMessage, String fragment) {
        assertTrue(errorMessage.contains(fragment), "Missing log fragment: " + fragment + "\nActual: " + errorMessage);
    }

    private void seedFieldCache(PaimonService service, List<DataField> fields) throws Exception {
        Field cacheField = PaimonService.class.getDeclaredField("paimonFieldCache");
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, List<DataField>> cache = (Map<String, List<DataField>>) cacheField.get(service);
        cache.put(CACHE_KEY, fields);
    }

    private void seedComputeHashKey(PaimonService service) throws Exception {
        Field cacheField = PaimonService.class.getDeclaredField("computeHashKey");
        cacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<String, Boolean> cache = (Map<String, Boolean>) cacheField.get(service);
        cache.put(TABLE_NAME, false);
    }

    private static void invokeHandleStreamInsert(PaimonService service, TapInsertRecordEvent event,
                                            StreamTableWrite writer, TapTable table) throws Exception {
        Method method = PaimonService.class.getDeclaredMethod("handleStreamInsert", TapInsertRecordEvent.class,
                StreamTableWrite.class, TapTable.class);
        method.setAccessible(true);
        try {
            method.invoke(service, event, writer, table);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    private static void invokeHandleStreamUpdate(PaimonService service, TapUpdateRecordEvent event,
                                            StreamTableWrite writer, TapTable table) throws Exception {
        Method method = PaimonService.class.getDeclaredMethod("handleStreamUpdate", TapUpdateRecordEvent.class,
                StreamTableWrite.class, TapTable.class);
        method.setAccessible(true);
        try {
            method.invoke(service, event, writer, table);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    private static void invokeHandleStreamDelete(PaimonService service, TapDeleteRecordEvent event,
                                            StreamTableWrite writer, TapTable table) throws Exception {
        Method method = PaimonService.class.getDeclaredMethod("handleStreamDelete", TapDeleteRecordEvent.class,
                StreamTableWrite.class, TapTable.class);
        method.setAccessible(true);
        try {
            method.invoke(service, event, writer, table);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    private static TapInsertRecordEvent buildInsertEvent(Map<String, Object> after) {
        return new TapInsertRecordEvent()
                .init()
                .table(TABLE_NAME)
                .after(after)
                .referenceTime(REFERENCE_TIME);
    }

    private static TapUpdateRecordEvent buildUpdateEvent(Map<String, Object> before, Map<String, Object> after) {
        return new TapUpdateRecordEvent()
                .init()
                .table(TABLE_NAME)
                .before(before)
                .after(after)
                .referenceTime(REFERENCE_TIME);
    }

    private static TapDeleteRecordEvent buildDeleteEvent(Map<String, Object> before) {
        return new TapDeleteRecordEvent()
                .init()
                .table(TABLE_NAME)
                .before(before)
                .referenceTime(REFERENCE_TIME);
    }

    @FunctionalInterface
    private interface ScenarioInvoker {
        void invoke(PaimonService service, StreamTableWrite writer, TapTable table) throws Exception;
    }

    private static class Scenario {
        private final String name;
        private final ScenarioInvoker invoker;
        private final String expectedRowKind;
        private final int expectedWriteCount;
        private final String expectedSecondFieldValue;
        private final String expectedExceptionMessage;
        private final String expectedSourceDataFragment;
        private final String expectedEventClassFragment;
        private final String expectedEventDetailFragment;
        private final String expectedRowValuesFragment;
        private final String expectedTruncationFragment;
        private final int failOnWriteCall;

        private Scenario(String name,
                         ScenarioInvoker invoker,
                         String expectedRowKind,
                         int expectedWriteCount,
                         String expectedSecondFieldValue,
                         String expectedExceptionMessage,
                         String expectedSourceDataFragment,
                         String expectedEventClassFragment,
                         String expectedEventDetailFragment,
                         String expectedRowValuesFragment,
                         String expectedTruncationFragment,
                         int failOnWriteCall) {
            this.name = name;
            this.invoker = invoker;
            this.expectedRowKind = expectedRowKind;
            this.expectedWriteCount = expectedWriteCount;
            this.expectedSecondFieldValue = expectedSecondFieldValue;
            this.expectedExceptionMessage = expectedExceptionMessage;
            this.expectedSourceDataFragment = expectedSourceDataFragment;
            this.expectedEventClassFragment = expectedEventClassFragment;
            this.expectedEventDetailFragment = expectedEventDetailFragment;
            this.expectedRowValuesFragment = expectedRowValuesFragment;
            this.expectedTruncationFragment = expectedTruncationFragment;
            this.failOnWriteCall = failOnWriteCall;
        }

        static Scenario insertNullRequiredField() {
            Map<String, Object> after = new LinkedHashMap<>();
            after.put("id", null);
            after.put("name", "Alice");
            TapInsertRecordEvent event = buildInsertEvent(after);
            return new Scenario(
                    "insert-null-id",
                    (service, writer, table) -> invokeHandleStreamInsert(service, event, writer, table),
                    "INSERT",
                    1,
                    "Alice",
                    "Required field id cannot be null",
                    "sourceData={id=null, name=Alice}",
                    "io.tapdata.entity.event.dml.TapInsertRecordEvent@",
                    "\"after\":{\"name\":\"Alice\"}",
                    "values={0:id=null, 1:name=\"Alice\"}",
                    null,
                    1
            );
        }

        static Scenario updateBeforeNullRequiredField() {
            Map<String, Object> before = new LinkedHashMap<>();
            before.put("id", null);
            before.put("name", "BeforeUpdate");
            Map<String, Object> after = new LinkedHashMap<>();
            after.put("id", 1001L);
            after.put("name", "AfterUpdate");
            TapUpdateRecordEvent event = buildUpdateEvent(before, after);
            return new Scenario(
                    "update-before-null-id",
                    (service, writer, table) -> invokeHandleStreamUpdate(service, event, writer, table),
                    "UPDATE_BEFORE",
                    1,
                    "BeforeUpdate",
                    "Required field id cannot be null",
                    "sourceData={id=null, name=BeforeUpdate}",
                    "io.tapdata.entity.event.dml.TapUpdateRecordEvent@",
                    "\"after\":{\"id\":1001,\"name\":\"AfterUpdate\"},\"before\":{\"name\":\"BeforeUpdate\"}",
                    "values={0:id=null, 1:name=\"BeforeUpdate\"}",
                    null,
                    1
            );
        }

        static Scenario updateAfterNullRequiredField() {
            Map<String, Object> before = new LinkedHashMap<>();
            before.put("id", 1001L);
            before.put("name", "BeforeUpdate");
            Map<String, Object> after = new LinkedHashMap<>();
            after.put("id", null);
            after.put("name", "AfterUpdate");
            TapUpdateRecordEvent event = buildUpdateEvent(before, after);
            return new Scenario(
                    "update-after-null-id",
                    (service, writer, table) -> invokeHandleStreamUpdate(service, event, writer, table),
                    "UPDATE_AFTER",
                    2,
                    "AfterUpdate",
                    "Required field id cannot be null",
                    "sourceData={id=null, name=AfterUpdate}",
                    "io.tapdata.entity.event.dml.TapUpdateRecordEvent@",
                    "\"after\":{\"name\":\"AfterUpdate\"},\"before\":{\"id\":1001,\"name\":\"BeforeUpdate\"}",
                    "values={0:id=null, 1:name=\"AfterUpdate\"}",
                    null,
                    2
            );
        }

        static Scenario insertNullRequiredFieldWithLongValue() {
            Map<String, Object> after = new LinkedHashMap<>();
            after.put("id", null);
            after.put("name", LONG_NAME);
            TapInsertRecordEvent event = buildInsertEvent(after);
            return new Scenario(
                    "insert-null-id-long-name",
                    (service, writer, table) -> invokeHandleStreamInsert(service, event, writer, table),
                    "INSERT",
                    1,
                    LONG_NAME,
                    "Required field id cannot be null",
                    "sourceData={id=null, name=" + LONG_NAME.substring(0, 32),
                    "io.tapdata.entity.event.dml.TapInsertRecordEvent@",
                    "\"after\":{\"name\":\"" + LONG_NAME.substring(0, 32),
                    "values={0:id=null, 1:name=\"" + LONG_NAME.substring(0, 32),
                    "...(len=300)",
                    1
            );
        }

        static Scenario deleteMissingRequiredField() {
            Map<String, Object> before = new LinkedHashMap<>();
            before.put("id", null);
            before.put("name", "DeleteTarget");
            TapDeleteRecordEvent event = buildDeleteEvent(before);
            return new Scenario(
                    "delete-null-id",
                    (service, writer, table) -> invokeHandleStreamDelete(service, event, writer, table),
                    "DELETE",
                    1,
                    "DeleteTarget",
                    "Required field id cannot be null",
                    "sourceData={id=null, name=DeleteTarget}",
                    "io.tapdata.entity.event.dml.TapDeleteRecordEvent@",
                    "\"before\":{\"name\":\"DeleteTarget\"}",
                    "values={0:id=null, 1:name=\"DeleteTarget\"}",
                    null,
                    1
            );
        }

        void invoke(PaimonService service, StreamTableWrite writer, TapTable table) throws Exception {
            invoker.invoke(service, writer, table);
        }

        String expectedRowKind() {
            return expectedRowKind;
        }

        int expectedWriteCount() {
            return expectedWriteCount;
        }

        String expectedSecondFieldValue() {
            return expectedSecondFieldValue;
        }

        String expectedExceptionMessage() {
            return expectedExceptionMessage;
        }

        String expectedSourceDataFragment() {
            return expectedSourceDataFragment;
        }

        String expectedEventClassFragment() {
            return expectedEventClassFragment;
        }

        String expectedEventDetailFragment() {
            return expectedEventDetailFragment;
        }

        String expectedRowValuesFragment() {
            return expectedRowValuesFragment;
        }

        String expectedTruncationFragment() {
            return expectedTruncationFragment;
        }

        int failOnWriteCall() {
            return failOnWriteCall;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static class FailingStreamTableWrite implements StreamTableWrite, AutoCloseable {

        private final AtomicInteger writeCount = new AtomicInteger();
        private final int failOnWriteCall;
        private final AtomicReference<GenericRow> failedRow = new AtomicReference<>();

        private FailingStreamTableWrite(int failOnWriteCall) {
            this.failOnWriteCall = failOnWriteCall;
        }

        @Override
        public List<CommitMessage> prepareCommit(boolean waitCompaction, long commitIdentifier) {
            return Collections.emptyList();
        }

        @Override
        public TableWrite withIOManager(org.apache.paimon.disk.IOManager ioManager) {
            return this;
        }

        @Override
        public TableWrite withWriteType(RowType writeType) {
            return this;
        }

        @Override
        public TableWrite withMemoryPoolFactory(org.apache.paimon.memory.MemoryPoolFactory memoryPoolFactory) {
            return this;
        }

        @Override
        public org.apache.paimon.data.BinaryRow getPartition(InternalRow row) {
            return null;
        }

        @Override
        public int getBucket(InternalRow row) {
            return 0;
        }

        @Override
        public void write(InternalRow row) {
            int current = writeCount.incrementAndGet();
            failedRow.set((GenericRow) row);
            if (current == failOnWriteCall) {
                throw new IllegalStateException("Required field id cannot be null");
            }
        }

        @Override
        public void write(InternalRow row, int bucket) {
            write(row);
        }

        @Override
        public void writeBundle(org.apache.paimon.data.BinaryRow partition, int bucket,
                                org.apache.paimon.io.BundleRecords bundle) {
        }

        @Override
        public void compact(org.apache.paimon.data.BinaryRow partition, int bucket, boolean fullCompaction) {
        }

        @Override
        public TableWrite withMetricRegistry(org.apache.paimon.metrics.MetricRegistry registry) {
            return this;
        }

        @Override
        public void close() {
        }

        private GenericRow getFailedRow() {
            return failedRow.get();
        }

        private int getWriteCount() {
            return writeCount.get();
        }
    }

    private static class CapturingLog implements Log {

        private String lastErrorMessage;
        private Throwable lastErrorThrowable;

        @Override
        public void debug(String message, Object... params) {
        }

        @Override
        public void info(String message, Object... params) {
        }

        @Override
        public void trace(String message, Object... params) {
        }

        @Override
        public void warn(String message, Object... params) {
        }

        @Override
        public void error(String message, Object... params) {
            lastErrorThrowable = extractThrowable(params);
            Object[] formattedParams = trimThrowable(params);
            lastErrorMessage = format(message, formattedParams);
        }

        @Override
        public void error(String message, Throwable throwable) {
            lastErrorThrowable = throwable;
            lastErrorMessage = message;
        }

        @Override
        public void fatal(String message, Object... params) {
        }

        private Throwable extractThrowable(Object[] params) {
            if (params == null || params.length == 0) {
                return null;
            }
            Object last = params[params.length - 1];
            return last instanceof Throwable ? (Throwable) last : null;
        }

        private Object[] trimThrowable(Object[] params) {
            if (params == null || params.length == 0) {
                return new Object[0];
            }
            if (!(params[params.length - 1] instanceof Throwable)) {
                return params;
            }
            Object[] trimmed = new Object[params.length - 1];
            System.arraycopy(params, 0, trimmed, 0, params.length - 1);
            return trimmed;
        }

        private String format(String template, Object[] params) {
            if (template == null) {
                return null;
            }
            if (params == null || params.length == 0) {
                return template;
            }
            StringBuilder builder = new StringBuilder();
            int paramIndex = 0;
            int cursor = 0;
            while (true) {
                int placeholder = template.indexOf("{}", cursor);
                if (placeholder < 0) {
                    builder.append(template.substring(cursor));
                    break;
                }
                builder.append(template, cursor, placeholder);
                if (paramIndex < params.length) {
                    builder.append(params[paramIndex++]);
                } else {
                    builder.append("{}");
                }
                cursor = placeholder + 2;
            }
            while (paramIndex < params.length) {
                builder.append(' ').append(params[paramIndex++]);
            }
            return builder.toString();
        }

        private String getLastErrorMessage() {
            return lastErrorMessage;
        }

        @SuppressWarnings("unused")
        private Throwable getLastErrorThrowable() {
            return lastErrorThrowable;
        }
    }
}




