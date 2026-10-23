package io.tapdata.connector.paimon.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.connector.paimon.PaimonConnector;
import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;
import io.tapdata.connector.paimon.schema.PaimonDataTypeConverter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.conversion.impl.TableFieldTypesGeneratorImpl;
import io.tapdata.entity.conversion.impl.TargetTypesGeneratorImpl;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.catalog.*;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.*;
import org.apache.paimon.table.source.*;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class PaimonShapedFloatingTest {
    @TempDir Path directory;
    private DefaultExpressionMatchingMap sourceTypes;
    private DefaultExpressionMatchingMap targetTypes;
    private TapCodecsFilterManager codecs;

    @BeforeEach
    void setUp() throws Exception {
        sourceTypes = types("fixtures/mysql-numeric-types.json");
        targetTypes = types("spec.json");
        TapCodecsRegistry registry = new TapCodecsRegistry();
        new PaimonConnector().registerCapabilities(new ConnectorFunctions(), registry);
        codecs = TapCodecsFilterManager.create(registry);
    }

    static Stream<String> fractionalDoubleDeclarations() {
        List<String> declarations = new ArrayList<>();
        for (int precision : new int[] {1, 2, 5, 6, 7, 8, 9, 10, 15, 16, 20, 38, 39, 255}) {
            for (int scale : new int[] {1, 2, 6, 7, 15, 30}) {
                if (scale <= precision) {
                    declarations.add("double(" + precision + "," + scale + ")");
                    declarations.add("double(" + precision + "," + scale + ") unsigned");
                }
            }
        }
        return declarations.stream();
    }

    @ParameterizedTest
    @MethodSource("fractionalDoubleDeclarations")
    void fractionalDoubleMustNotSelectFloat(String declaration) {
        TapField target = target(source(declaration));
        assertEquals(DataTypes.DOUBLE(), PaimonDataTypeConverter.toPaimonDataType(target));
    }

    @ParameterizedTest
    @ValueSource(strings = {"float", "double(5,2)", "double(6,6)", "double(8,2)", "double(8,2) unsigned"})
    void ordinaryFloatingMappingsMustNotReportPrecisionLoss(String declaration) {
        TapField source = source(declaration);
        var mapping = new TargetTypesGeneratorImpl().convert(
                new LinkedHashMap<>(Map.of("v", source)), targetTypes, codecs);
        assertTrue(mapping.getResultItems() == null || mapping.getResultItems().isEmpty(), declaration);
    }

    @ParameterizedTest
    @ValueSource(strings = {"float", "double", "double(5,2)", "double(6,6)",
            "double(8,2)", "double(8,2) unsigned"})
    void floatingMappingMustPreserveIncomingDoubleThroughCodecsAndBinaryRows(String declaration) {
        TapField source = source(declaration);
        DataType physical = PaimonDataTypeConverter.toPaimonDataType(target(source));
        assertEquals(DataTypes.DOUBLE(), physical);
        double[] values = declaration.startsWith("double(6,6)")
                ? new double[] {0.123456, 0.999999}
                : declaration.startsWith("double(5,2)")
                    ? new double[] {123.45, 999.99}
                    : new double[] {999999.99, 999999.98, 999999.97, 123.45, 0.0};
        for (double value : values) {
            assertDoubleBits(value, source, physical);
            if (!declaration.endsWith("unsigned")) assertDoubleBits(-value, source, physical);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"float(8,0)", "double(8,0)", "double(8,0) unsigned"})
    void zeroScaleFloatingMustReachPhysicalIntegerChecksWithoutPriorNarrowing(String declaration) {
        TapField source = source(declaration);
        DataType physical = PaimonDataTypeConverter.toPaimonDataType(target(source));
        assertEquals(DataTypes.INT(), physical);
        assertEquals(99999999, PaimonDataTypeConverter.toInternalValue("v", codecValue(source, 99999999D), physical));
        for (double value : new double[] {1.0E14, -1.0E14, 12.5}) {
            Object decoded = codecValue(source, value);
            assertEquals(value, assertInstanceOf(Double.class, decoded));
            assertThrows(PaimonFatalWriteException.class,
                    () -> PaimonDataTypeConverter.toInternalValue("v", decoded, physical));
        }
    }

    @Test
    void nativeFloatMustRemainExplicitlyAvailable() {
        TapField field = new TapField("v", "FLOAT");
        new TableFieldTypesGeneratorImpl().autoFill(field, targetTypes);
        assertNotNull(field.getTapType());
        assertEquals(DataTypes.FLOAT(), PaimonDataTypeConverter.toPaimonDataType(field));
        assertEquals(1.25F, PaimonDataTypeConverter.toInternalValue("v", 1.25F, DataTypes.FLOAT()));
    }

    private TapField source(String declaration) {
        TapField field = new TapField("v", declaration);
        new TableFieldTypesGeneratorImpl().autoFill(field, sourceTypes);
        return field;
    }

    private TapField target(TapField source) {
        var mapping = new TargetTypesGeneratorImpl().convert(
                new LinkedHashMap<>(Map.of(source.getName(), source)), targetTypes, codecs);
        return mapping.getData().get(source.getName());
    }

    private Object codecValue(TapField source, double value) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("v", value);
        codecs.transformToTapValueMap(row, Map.of("v", source));
        codecs.transformFromTapValueMap(row);
        return row.get("v");
    }

    private void assertDoubleBits(double value, TapField source, DataType physical) {
        Object internal = PaimonDataTypeConverter.toInternalValue("v", codecValue(source, value), physical);
        double actual = new InternalRowSerializer(physical).toBinaryRow(GenericRow.of(internal)).getDouble(0);
        assertEquals(Double.doubleToLongBits(value), Double.doubleToLongBits(actual));
    }

    @Test
    void mappedSmallPrecisionDoubleMustSurviveTableCommitAndReload() throws Exception {
        TapField source = source("double(8,2)");
        TapTable tapTable = new TapTable("small_precision_double")
                .add(new TapField("id", "INT").primaryKeyPos(1)).add(target(source));
        Options options = new Options();
        options.set("warehouse", directory.resolve("warehouse").toString());
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            PaimonConfig config = new PaimonConfig();
            config.setDatabase("default"); config.setBucketMode("fixed"); config.setBucketCount(1);
            config.setHashKey(false); config.setPartitionKey(Collections.emptyList());
            try (PaimonService service = new PaimonService(config, mock(Log.class))) {
                Field catalogField = PaimonService.class.getDeclaredField("catalog");
                catalogField.setAccessible(true); catalogField.set(service, catalog);
                service.startForTest();
                assertTrue(service.createTable(tapTable));
                Identifier identifier = Identifier.create("default", tapTable.getId());
                FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
                assertEquals(DataTypes.DOUBLE(), table.rowType().getField("v").type());
                Method toRow = PaimonService.class.getDeclaredMethod(
                        "convertToGenericRow", Map.class, TapTable.class, Identifier.class);
                toRow.setAccessible(true);
                Method fromRow = PaimonService.class.getDeclaredMethod(
                        "convertInternalRowToMap", InternalRow.class, List.class, Map.class);
                fromRow.setAccessible(true);
                double[] values = {999999.99, 999999.98, 999999.97, -999999.99, 123.45};
                StreamWriteBuilder builder = table.newStreamWriteBuilder();
                try (StreamTableWrite write = builder.newWrite(); StreamTableCommit commit = builder.newCommit()) {
                    for (int id = 0; id < values.length; id++) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        row.put("id", id); row.put("v", codecValue(source, values[id]));
                        write.write((GenericRow) toRow.invoke(service, row, tapTable, identifier));
                    }
                    commit.commit(1L, write.prepareCommit(false, 1L));
                }
                catalog.invalidateTable(identifier);
                table = (FileStoreTable) catalog.getTable(identifier);
                ReadBuilder read = table.newReadBuilder();
                Set<Integer> seen = new HashSet<>();
                for (Split split : read.newScan().plan().splits()) {
                    try (RecordReader<InternalRow> reader = read.newRead().createReader(split)) {
                        RecordReader.RecordIterator<InternalRow> batch;
                        while ((batch = reader.readBatch()) != null) {
                            try {
                                InternalRow row;
                                while ((row = batch.next()) != null) {
                                    @SuppressWarnings("unchecked")
                                    Map<String, Object> actual = (Map<String, Object>) fromRow.invoke(
                                            service, row, table.rowType().getFields(), Collections.emptyMap());
                                    int id = ((Number) actual.get("id")).intValue();
                                    assertTrue(seen.add(id));
                                    assertEquals(Double.doubleToLongBits(values[id]),
                                            Double.doubleToLongBits(assertInstanceOf(Double.class, actual.get("v"))));
                                }
                            } finally { batch.releaseBatch(); }
                        }
                    }
                }
                assertEquals(values.length, seen.size());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"float(20,6)", "float(20,6) unsigned", "double(20,6)",
            "double(20,6) unsigned", "float(30,30)", "double(255,30)", "double(20,0)"})
    void shapedFloatingSourcesMustNotBeNarrowedToFixedDecimal(String sourceType) throws Exception {
        TapField source = new TapField("v", sourceType);
        new TableFieldTypesGeneratorImpl().autoFill(source, types("fixtures/mysql-numeric-types.json"));
        TapCodecsRegistry registry = new TapCodecsRegistry();
        new PaimonConnector().registerCapabilities(new ConnectorFunctions(), registry);
        LinkedHashMap<String, TapField> fields = new LinkedHashMap<>();
        fields.put("v", source);
        TapField target = new TargetTypesGeneratorImpl().convert(fields, types("spec.json"),
                TapCodecsFilterManager.create(registry)).getData().get("v");
        assertEquals(DataTypes.DOUBLE(), PaimonDataTypeConverter.toPaimonDataType(target));
        // Connector/J 8.0.28 + local MySQL 5.7 reproduced both positive and negative 1e14.
        List<Number> values = new ArrayList<>();
        if (sourceType.startsWith("float")) {
            values.add(sourceType.contains("30,30") ? 0.125F : 1.0E14F);
        } else {
            values.add(sourceType.contains("255,30") ? 1.0E225D : 1.0E14D);
        }
        if (!sourceType.contains("unsigned")) values.add(-values.get(0).doubleValue());
        for (Number value : values) {
            Object internal = PaimonDataTypeConverter.toInternalValue("v", value,
                    PaimonDataTypeConverter.toPaimonDataType(target));
            assertEquals(value.doubleValue(), new InternalRowSerializer(DataTypes.DOUBLE())
                    .toBinaryRow(GenericRow.of(internal)).getDouble(0));
        }
    }

    private DefaultExpressionMatchingMap types(String resource) throws Exception {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(resource)) {
            assertNotNull(input);
            ObjectMapper mapper = new ObjectMapper();
            return DefaultExpressionMatchingMap.map(mapper.convertValue(mapper.readTree(input).get("dataTypes"),
                    new TypeReference<LinkedHashMap<String, DataMap>>() {}));
        }
    }
}
