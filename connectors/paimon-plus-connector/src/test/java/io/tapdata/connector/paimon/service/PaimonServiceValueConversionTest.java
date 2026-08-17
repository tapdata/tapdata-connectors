package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.write.PaimonTableCommitter;
import io.tapdata.connector.paimon.write.PaimonTableWriteContext;
import io.tapdata.connector.paimon.write.bucket.PaimonBucketWriterStrategy;

import io.tapdata.connector.paimon.schema.PaimonWriteSemanticContractTestFactory;

import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonServiceValueConversionTest {

    private static final String DATABASE = "default";
    private static final String TABLE_NAME = "integer_values";
    private static final String TABLE_KEY = DATABASE + "." + TABLE_NAME;

    private PaimonService service;
    private PaimonTableWriteContext writeContext;
    private PaimonBucketWriterStrategy writerStrategy;
    private TapTable tapTable;
    private TapConnectorContext connectorContext;

    @BeforeEach
    void setUp() throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase(DATABASE);
        config.setBatchAccumulationSize(100);
        config.setCommitIntervalMs(0);
        config.setEnableAsyncCommit(false);
        service = new PaimonService(config, mock(Log.class));
        service.startForTest();

        writerStrategy = mock(PaimonBucketWriterStrategy.class);
        when(writerStrategy.bucketMode()).thenReturn(BucketMode.HASH_FIXED);
        when(writerStrategy.writeSemanticContract())
                .thenReturn(PaimonWriteSemanticContractTestFactory.forMode(BucketMode.HASH_FIXED));
        writeContext =
                new PaimonTableWriteContext(
                        TABLE_KEY,
                        TABLE_NAME,
                        "stable-user",
                        writerStrategy,
                        mock(PaimonTableCommitter.class),
                        null,
                        Collections.emptyList(),
                        0L);
        tableContexts(service).put(TABLE_KEY, writeContext);
        fieldCache(service)
                .put(
                        TABLE_KEY,
                        Collections.singletonList(new DataField(0, "id", DataTypes.INT())));

        tapTable = mock(TapTable.class);
        when(tapTable.getName()).thenReturn(TABLE_NAME);
        when(tapTable.primaryKeys(true)).thenReturn(Collections.emptyList());

        connectorContext = mock(TapConnectorContext.class);
        when(connectorContext.getStateMap()).thenReturn(mock(KVMap.class));
        when(connectorContext.getLog()).thenReturn(mock(Log.class));
    }

    @AfterEach
    void tearDown() throws Exception {
        writeContext.close();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("integerInputs")
    void writeRecordsMustNormalizeIntegerFieldBeforePaimonSerialization(
            String scenario, Number sourceValue, int expectedValue) throws Exception {
        TapInsertRecordEvent event =
                new TapInsertRecordEvent()
                        .init()
                        .table(TABLE_NAME)
                        .after(Collections.singletonMap("id", sourceValue));
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "INITIAL_SYNC");

        service.writeRecords(Collections.singletonList(event), tapTable, connectorContext);

        ArgumentCaptor<InternalRow> rowCaptor = ArgumentCaptor.forClass(InternalRow.class);
        verify(writerStrategy).write(rowCaptor.capture());
        GenericRow convertedRow = assertInstanceOf(GenericRow.class, rowCaptor.getValue());
        Integer convertedValue = assertInstanceOf(Integer.class, convertedRow.getField(0));
        assertEquals(expectedValue, convertedValue.intValue());

        BinaryRow serializedRow =
                new InternalRowSerializer(DataTypes.INT()).toBinaryRow(convertedRow);
        assertEquals(expectedValue, serializedRow.getInt(0));
    }

    @Test
    void fractionalIntegerMustFailBeforeWriterIngressAndFenceFollowingWrites() throws Exception {
        TapInsertRecordEvent invalidEvent = insertEvent(42.5D);

        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () ->
                                service.writeRecords(
                                        Collections.singletonList(invalidEvent),
                                        tapTable,
                                        connectorContext));

        assertTrueConversionMessage(thrown, "id", "INT", Double.class);
        assertFalse(thrown.getMessage().contains("42.5"));
        verify(writerStrategy, never()).write(any());

        PaimonFatalWriteException replay =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () ->
                                service.writeRecords(
                                        Collections.singletonList(insertEvent(1)),
                                        tapTable,
                                        connectorContext));
        assertSame(thrown, replay);
    }

    @Test
    void legacyPhysicalStringColumnMustKeepAcceptingNumericCdcValues() throws Exception {
        fieldCache(service)
                .put(
                        TABLE_KEY,
                        Collections.singletonList(
                                new DataField(0, "id", DataTypes.STRING())));

        service.writeRecords(
                Collections.singletonList(insertEvent(42L)), tapTable, connectorContext);

        ArgumentCaptor<InternalRow> rowCaptor = ArgumentCaptor.forClass(InternalRow.class);
        verify(writerStrategy).write(rowCaptor.capture());
        GenericRow convertedRow = assertInstanceOf(GenericRow.class, rowCaptor.getValue());
        BinaryString convertedValue =
                assertInstanceOf(BinaryString.class, convertedRow.getField(0));
        assertEquals("42", convertedValue.toString());
        BinaryRow serializedRow =
                new InternalRowSerializer(DataTypes.STRING()).toBinaryRow(convertedRow);
        assertEquals("42", serializedRow.getString(0).toString());
    }

    @Test
    void existingPhysicalDecimalColumnMustKeepUsingActualRowType() throws Exception {
        fieldCache(service)
                .put(
                        TABLE_KEY,
                        Collections.singletonList(
                                new DataField(0, "id", DataTypes.DECIMAL(10, 0))));

        service.writeRecords(
                Collections.singletonList(insertEvent(new BigDecimal("12.5"))),
                tapTable,
                connectorContext);

        ArgumentCaptor<InternalRow> rowCaptor = ArgumentCaptor.forClass(InternalRow.class);
        verify(writerStrategy).write(rowCaptor.capture());
        GenericRow convertedRow = assertInstanceOf(GenericRow.class, rowCaptor.getValue());
        Decimal convertedValue = assertInstanceOf(Decimal.class, convertedRow.getField(0));
        assertEquals("13", convertedValue.toString());

        BinaryRow serializedRow =
                new InternalRowSerializer(DataTypes.DECIMAL(10, 0)).toBinaryRow(convertedRow);
        assertEquals("13", serializedRow.getDecimal(0, 10, 0).toString());
    }

    @Test
    void nativeComplexPhysicalColumnMustFailBeforePaimonWriter() throws Exception {
        fieldCache(service)
                .put(
                        TABLE_KEY,
                        Collections.singletonList(
                                new DataField(
                                        0,
                                        "id",
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))));

        PaimonFatalWriteException thrown =
                assertThrows(
                        PaimonFatalWriteException.class,
                        () ->
                                service.writeRecords(
                                        Collections.singletonList(
                                                insertEvent(
                                                        Collections.singletonMap(
                                                                "sensitive-key", "sensitive-value"))),
                                        tapTable,
                                        connectorContext));

        assertTrueConversionMessage(
                thrown,
                "id",
                "MAP<STRING, STRING>",
                Collections.singletonMap("key", "value").getClass());
        org.junit.jupiter.api.Assertions.assertTrue(thrown.getMessage().contains("JSON STRING"));
        assertFalse(thrown.getMessage().contains("sensitive-key"));
        assertFalse(thrown.getMessage().contains("sensitive-value"));
        verify(writerStrategy, never()).write(any());
    }

    private static TapInsertRecordEvent insertEvent(Object value) {
        TapInsertRecordEvent event =
                new TapInsertRecordEvent()
                        .init()
                        .table(TABLE_NAME)
                        .after(Collections.singletonMap("id", value));
        event.addInfo(TapRecordEvent.INFO_KEY_SYNC_STAGE, "INITIAL_SYNC");
        return event;
    }

    private static void assertTrueConversionMessage(
            PaimonFatalWriteException thrown,
            String fieldName,
            String targetType,
            Class<?> sourceType) {
        String message = thrown.getMessage();
        org.junit.jupiter.api.Assertions.assertTrue(
                message.contains("PAIMON_VALUE_CONVERSION_FAILED"));
        org.junit.jupiter.api.Assertions.assertTrue(message.contains("field=" + fieldName));
        org.junit.jupiter.api.Assertions.assertTrue(message.contains("target=" + targetType));
        org.junit.jupiter.api.Assertions.assertTrue(
                message.contains("source=" + sourceType.getName()));
    }

    private static Stream<Arguments> integerInputs() {
        return Stream.of(
                Arguments.of("Long value", 42L, 42),
                Arguments.of(
                        "Long Integer.MIN_VALUE", (long) Integer.MIN_VALUE, Integer.MIN_VALUE),
                Arguments.of(
                        "Long Integer.MAX_VALUE", (long) Integer.MAX_VALUE, Integer.MAX_VALUE),
                Arguments.of("Integer value", 7, 7));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, PaimonTableWriteContext> tableContexts(PaimonService service)
            throws Exception {
        Field field = PaimonService.class.getDeclaredField("tableWriteContexts");
        field.setAccessible(true);
        return (Map<String, PaimonTableWriteContext>) field.get(service);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, List<DataField>> fieldCache(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("paimonFieldCache");
        field.setAccessible(true);
        return (Map<String, List<DataField>>) field.get(service);
    }
}
