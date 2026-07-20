package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonServiceWriteErrorLogTest {

    private static final String DATABASE = "test_db";
    private static final String TABLE_NAME = "customer_table";
    private static final String CACHE_KEY = DATABASE + "." + TABLE_NAME;
    private static final long REFERENCE_TIME = 123456789L;
    private static final String LONG_NAME = repeat('x', 300);

    @Test
    void legacySyntheticHashMustKeepMissingAndNullPrimaryKeyEncoding() {
        PaimonService service = new PaimonService(new PaimonConfig(), mock(Log.class));
        List<String> keys = Arrays.asList("pk1", "pk2", "pk3");
        Map<String, Object> missing = new LinkedHashMap<>();
        missing.put("pk1", 1);
        missing.put("pk3", 3);
        Map<String, Object> explicitNull = new LinkedHashMap<>(missing);
        explicitNull.put("pk2", null);

        assertEquals("5989c034788371db039a59ebbdbc5ff8", service.toHash(keys, missing));
        assertEquals(service.toHash(keys, missing), service.toHash(keys, explicitNull));
    }

    @Test
    void syntheticHashFailureMustNotExposeSourceValues() {
        PaimonService service = new PaimonService(new PaimonConfig(), mock(Log.class));
        String secret = "primary-key-secret-sentinel";
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("id", new Object() {
            @Override
            public String toString() {
                throw new IllegalStateException(secret);
            }
        });

        RuntimeException error = assertThrows(
                RuntimeException.class,
                () -> service.toHash(Collections.singletonList("id"), data));

        assertFalse(error.getMessage().contains(secret));
        assertFalse(error.toString().contains(secret));
    }

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
        seedFieldCache(service, Arrays.asList(
                new DataField(0, "id", DataTypes.BIGINT()),
                new DataField(1, "name", DataTypes.STRING())
        ));

        try (FailingWriteFixture fixture = new FailingWriteFixture(scenario.failOnWriteCall())) {
            Exception thrown =
                    assertThrows(
                            Exception.class,
                            () -> scenario.invoke(service, fixture.context(), table));
            assertEquals(scenario.expectedExceptionMessage(), thrown.getMessage());

            assertNotNull(fixture.getFailedRow());
            assertEquals(scenario.expectedRowKind(), fixture.getFailedRow().getRowKind().name());
            assertNull(fixture.getFailedRow().getField(0));
            assertEquals(
                    scenario.expectedSecondFieldValue(),
                    fixture.getFailedRow().getField(1).toString());
            assertEquals(scenario.expectedWriteCount(), fixture.getWriteCount());
        }

        String errorMessage = log.getLastErrorMessage();
        assertNotNull(errorMessage);
        assertTrue(
                errorMessage.contains(
                        "Failed to write row to Paimon. table=customer_table, bucket=connector-managed"));
        assertTrue(errorMessage.contains(scenario.expectedSourceDataFragment()));
        assertTrue(errorMessage.contains("fieldMapping=[0:id, 1:name]"));
        assertTrue(errorMessage.contains(scenario.expectedRowValuesFragment()));
        assertTrue(errorMessage.contains("rowKind=" + scenario.expectedRowKind()));
        assertFalse(errorMessage.contains("Alice"));
        assertFalse(errorMessage.contains("BeforeUpdate"));
        assertFalse(errorMessage.contains("AfterUpdate"));
        assertFalse(errorMessage.contains(LONG_NAME.substring(0, 32)));
    }

    private static Stream<Arguments> writeFailureScenarios() {
        return Stream.of(
                Arguments.of(Scenario.insertNullRequiredField()),
                Arguments.of(Scenario.updateBeforeNullRequiredField()),
                Arguments.of(Scenario.updateAfterNullRequiredField()),
                Arguments.of(Scenario.insertNullRequiredFieldWithLongValue())
        );
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
                                            PaimonTableWriteContext writeContext, TapTable table) throws Exception {
        Method method = PaimonService.class.getDeclaredMethod("handleStreamInsert", TapInsertRecordEvent.class,
                PaimonTableWriteContext.class, TapTable.class, Log.class);
        method.setAccessible(true);
        try {
            method.invoke(service, event, writeContext, table, serviceLog(service));
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    private static void invokeHandleStreamUpdate(PaimonService service, TapUpdateRecordEvent event,
                                            PaimonTableWriteContext writeContext, TapTable table) throws Exception {
        Method method = PaimonService.class.getDeclaredMethod("handleStreamUpdate", TapUpdateRecordEvent.class,
                PaimonTableWriteContext.class, TapTable.class, Log.class);
        method.setAccessible(true);
        try {
            method.invoke(service, event, writeContext, table, serviceLog(service));
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    private static Log serviceLog(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("log");
        field.setAccessible(true);
        return (Log) field.get(service);
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

    private static String repeat(char value, int count) {
        StringBuilder builder = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.append(value);
        }
        return builder.toString();
    }

    @FunctionalInterface
    private interface ScenarioInvoker {
        void invoke(PaimonService service, PaimonTableWriteContext writeContext, TapTable table)
                throws Exception;
    }

    private static class Scenario {
        private final String name;
        private final ScenarioInvoker invoker;
        private final String expectedRowKind;
        private final int expectedWriteCount;
        private final String expectedSecondFieldValue;
        private final String expectedExceptionMessage;
        private final String expectedSourceDataFragment;
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
            this.expectedRowValuesFragment = expectedRowValuesFragment;
            this.expectedTruncationFragment = expectedTruncationFragment;
            this.failOnWriteCall = failOnWriteCall;
        }

        static Scenario insertNullRequiredField() {
            Map<String, Object> after = new LinkedHashMap<>();
            after.put("id", null);
            after.put("name", "Alice");
            TapInsertRecordEvent event = new PaimonServiceWriteErrorLogTest().buildInsertEvent(after);
            return new Scenario(
                    "insert-null-id",
                    (service, writer, table) -> invokeHandleStreamInsert(service, event, writer, table),
                    "INSERT",
                    1,
                    "Alice",
                    "Required field id cannot be null",
                    "sourceData={id=null, name=String(len=5)}",
                    "valueMetadata={0:id=null, 1:name=BinaryString(len=5)}",
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
            TapUpdateRecordEvent event = new PaimonServiceWriteErrorLogTest().buildUpdateEvent(before, after);
            return new Scenario(
                    "update-before-null-id",
                    (service, writer, table) -> invokeHandleStreamUpdate(service, event, writer, table),
                    "UPDATE_BEFORE",
                    1,
                    "BeforeUpdate",
                    "Required field id cannot be null",
                    "sourceData={id=null, name=String(len=12)}",
                    "valueMetadata={0:id=null, 1:name=BinaryString(len=12)}",
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
            TapUpdateRecordEvent event = new PaimonServiceWriteErrorLogTest().buildUpdateEvent(before, after);
            return new Scenario(
                    "update-after-null-id",
                    (service, writer, table) -> invokeHandleStreamUpdate(service, event, writer, table),
                    "UPDATE_AFTER",
                    2,
                    "AfterUpdate",
                    "Required field id cannot be null",
                    "sourceData={id=null, name=String(len=11)}",
                    "valueMetadata={0:id=null, 1:name=BinaryString(len=11)}",
                    null,
                    2
            );
        }

        static Scenario insertNullRequiredFieldWithLongValue() {
            Map<String, Object> after = new LinkedHashMap<>();
            after.put("id", null);
            after.put("name", LONG_NAME);
            TapInsertRecordEvent event = new PaimonServiceWriteErrorLogTest().buildInsertEvent(after);
            return new Scenario(
                    "insert-null-id-long-name",
                    (service, writer, table) -> invokeHandleStreamInsert(service, event, writer, table),
                    "INSERT",
                    1,
                    LONG_NAME,
                    "Required field id cannot be null",
                    "sourceData={id=null, name=String(len=300)}",
                    "valueMetadata={0:id=null, 1:name=BinaryString(len=300)}",
                    null,
                    1
            );
        }

        void invoke(
                PaimonService service, PaimonTableWriteContext writeContext, TapTable table)
                throws Exception {
            invoker.invoke(service, writeContext, table);
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

    private static class FailingWriteFixture implements AutoCloseable {

        private final FailingWriterStrategy strategy;
        private final PaimonTableWriteContext context;

        private FailingWriteFixture(int failOnWriteCall) {
            this.strategy = new FailingWriterStrategy(failOnWriteCall);
            this.context =
                    new PaimonTableWriteContext(
                            CACHE_KEY,
                            TABLE_NAME,
                            "write-error-log-test",
                            strategy,
                            new NoopCommitter(),
                            null,
                            Collections.emptyList(),
                            0L);
        }

        private PaimonTableWriteContext context() {
            return context;
        }

        private GenericRow getFailedRow() {
            return strategy.getFailedRow();
        }

        private int getWriteCount() {
            return strategy.getWriteCount();
        }

        @Override
        public void close() throws Exception {
            context.close();
        }
    }

    private static class FailingWriterStrategy implements PaimonBucketWriterStrategy {

        private final AtomicInteger writeCount = new AtomicInteger();
        private final int failOnWriteCall;
        private final AtomicReference<GenericRow> failedRow = new AtomicReference<>();

        private FailingWriterStrategy(int failOnWriteCall) {
            this.failOnWriteCall = failOnWriteCall;
        }

        @Override
        public BucketMode bucketMode() {
            return BucketMode.HASH_FIXED;
        }

        @Override
        public void validateRoutingRow(InternalRow row, String operation) {
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
        public List<CommitMessage> prepareCommit(long commitIdentifier) {
            return Collections.emptyList();
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

    private static class NoopCommitter implements PaimonTableCommitter {

        @Override
        public int filterAndCommit(Map<Long, List<CommitMessage>> pendingCommits) {
            return pendingCommits.size();
        }

        @Override
        public void close() {}
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
            lastErrorThrowable = extractThrowable(params);
            Object[] formattedParams = trimThrowable(params);
            lastErrorMessage = format(message, formattedParams);
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
