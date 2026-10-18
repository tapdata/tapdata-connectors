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
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.result.TapResult;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.type.TapNumber;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class PaimonNumericTypeMappingTest {

    @TempDir Path tempDir;

    private DefaultExpressionMatchingMap mysqlTypes;
    private DefaultExpressionMatchingMap paimonTypes;
    private TapCodecsFilterManager codecs;

    @BeforeEach
    void setUp() throws Exception {
        // Self-contained copies of the MySQL numeric declarations, including the missing flag.
        mysqlTypes = typeMap("fixtures/mysql-numeric-types.json");
        paimonTypes = typeMap("spec.json");
        TapCodecsRegistry registry = new TapCodecsRegistry();
        new PaimonConnector().registerCapabilities(new ConnectorFunctions(), registry);
        codecs = TapCodecsFilterManager.create(registry);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "bigint unsigned",
        "bigint(20) unsigned",
        "bigint unsigned zerofill",
        "bigint(20) unsigned zerofill"
    })
    void unsignedBigintMustMapToExactDecimalWithoutPrecisionWarnings(String sourceType) {
        TapField source = mysqlField(sourceType);
        TapNumber number = (TapNumber) source.getTapType();
        assertEquals(20, number.getPrecision());
        assertEquals(BigDecimal.ZERO, number.getMinValue());
        assertEquals(new BigDecimal("18446744073709551615"), number.getMaxValue());
        if (sourceType.endsWith("zerofill")) {
            assertEquals(Boolean.TRUE, number.getUnsigned());
        } else {
            // The plain MySQL UNSIGNED declaration currently omits this flag.
            assertNull(number.getUnsigned());
        }

        assertMapping(source, "DECIMAL(20,0)");
    }

    @ParameterizedTest
    @CsvSource({
        "tinyint, TINYINT",
        "tinyint unsigned, SMALLINT",
        "smallint, SMALLINT",
        "smallint unsigned, INTEGER",
        "mediumint, INTEGER",
        "mediumint unsigned, INTEGER",
        "int, INTEGER",
        "int unsigned, BIGINT",
        "bigint, BIGINT",
        "'decimal(10,0)', 'DECIMAL(10,0)'",
        "'decimal(19,0)', 'DECIMAL(19,0)'",
        "'decimal(20,0)', 'DECIMAL(20,0)'",
        "'decimal(21,0)', 'DECIMAL(21,0)'",
        "'decimal(38,0)', 'DECIMAL(38,0)'",
        "'decimal(20,6)', 'DECIMAL(20,6)'",
        "'decimal(38,18)', 'DECIMAL(38,18)'"
    })
    void existingNumericMappingsMustPreserveTheirPhysicalTypesAndPrecision(
            String sourceType, String expectedType) {
        assertMapping(mysqlField(sourceType), expectedType);
    }

    @Test
    void bareDecimalMustKeepItsExistingPrecisionAndScaleDefaults() {
        TapField field = new TapField("amount", "DECIMAL");
        new TableFieldTypesGeneratorImpl().autoFill(field, paimonTypes);

        TapNumber number = (TapNumber) field.getTapType();
        assertEquals(38, number.getPrecision());
        assertEquals(10, number.getScale());
        assertEquals(Boolean.TRUE, number.getFixed());
    }

    @Test
    void mappedUnsignedBigintMustRoundTripThroughNewPhysicalTable() throws Exception {
        String tableName = "unsigned_bigint_values";
        TapField target = assertMapping(mysqlField("bigint(20) unsigned"), "DECIMAL(20,0)");
        TapTable tapTable = new TapTable(tableName)
                .add(new TapField("id", "INT").primaryKeyPos(1))
                .add(target);
        List<BigInteger> values = Arrays.asList(
                BigInteger.ZERO,
                BigInteger.valueOf(Long.MAX_VALUE),
                new BigInteger("9223372036854775808"),
                new BigInteger("18446744073709551615"));

        try (Catalog catalog = catalog()) {
            PaimonService service = service(catalog);
            try {
                assertTrue(service.createTable(tapTable));
                FileStoreTable table = (FileStoreTable) catalog.getTable(
                        Identifier.create("default", tableName));
                DataType physicalType = table.rowType().getField(target.getName()).type();
                assertEquals(DataTypes.DECIMAL(20, 0), physicalType);

                Map<Integer, BigInteger> expected = new LinkedHashMap<>();
                StreamWriteBuilder builder = table.newStreamWriteBuilder();
                try (StreamTableWrite write = builder.newWrite();
                        StreamTableCommit commit = builder.newCommit()) {
                    for (int id = 0; id < values.size(); id++) {
                        BigInteger value = values.get(id);
                        Object internal = PaimonDataTypeConverter.toInternalValue(
                                target.getName(), value, physicalType);
                        write.write(GenericRow.of(id, internal));
                        expected.put(id, value);
                    }
                    commit.commit(1L, write.prepareCommit(false, 1L));
                }

                catalog.invalidateTable(Identifier.create("default", tableName));
                FileStoreTable reloaded = (FileStoreTable) catalog.getTable(
                        Identifier.create("default", tableName));
                assertEquals(expected, readDecimalValues(reloaded));
            } finally {
                service.close();
            }
        }
    }

    @Test
    void existingBigintColumnMustRemainUnchangedAndRejectUnsignedOverflow() throws Exception {
        String tableName = "existing_signed_bigint";
        Identifier identifier = Identifier.create("default", tableName);
        TapField target = assertMapping(mysqlField("bigint unsigned"), "DECIMAL(20,0)");
        try (Catalog catalog = catalog()) {
            catalog.createDatabase("default", true);
            catalog.createTable(identifier, Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column(target.getName(), DataTypes.BIGINT())
                    .primaryKey("id")
                    .option("bucket", "1")
                    .build(), false);
            PaimonService service = service(catalog);
            try {
                assertFalse(service.createTable(new TapTable(tableName)
                        .add(new TapField("id", "INT").primaryKeyPos(1)).add(target)));
                catalog.invalidateTable(identifier);
                DataType physicalType = catalog.getTable(identifier).rowType()
                        .getField(target.getName()).type();
                assertEquals(DataTypes.BIGINT(), physicalType);
                assertThrows(PaimonFatalWriteException.class,
                        () -> PaimonDataTypeConverter.toInternalValue(target.getName(),
                                new BigInteger("9223372036854775808"), physicalType));
            } finally {
                service.close();
            }
        }
    }

    private Catalog catalog() {
        Options options = new Options();
        options.set("warehouse", tempDir.resolve("warehouse").toString());
        return CatalogFactory.createCatalog(CatalogContext.create(options));
    }

    private PaimonService service(Catalog catalog) throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        config.setBucketMode("fixed");
        config.setBucketCount(1);
        config.setHashKey(false);
        config.setPartitionKey(Collections.emptyList());
        PaimonService service = new PaimonService(config, mock(Log.class));
        Field field = PaimonService.class.getDeclaredField("catalog");
        field.setAccessible(true);
        field.set(service, catalog);
        service.startForTest();
        return service;
    }

    private Map<Integer, BigInteger> readDecimalValues(FileStoreTable table) throws Exception {
        Map<Integer, BigInteger> values = new LinkedHashMap<>();
        ReadBuilder builder = table.newReadBuilder();
        for (Split split : builder.newScan().plan().splits()) {
            try (RecordReader<InternalRow> reader = builder.newRead().createReader(split)) {
                RecordReader.RecordIterator<InternalRow> batch;
                while ((batch = reader.readBatch()) != null) {
                    try {
                        InternalRow row;
                        while ((row = batch.next()) != null) {
                            BigInteger value = row.getDecimal(1, 20, 0)
                                    .toBigDecimal().toBigIntegerExact();
                            assertNull(values.put(row.getInt(0), value), "Duplicate primary key");
                        }
                    } finally {
                        batch.releaseBatch();
                    }
                }
            }
        }
        return values;
    }

    private TapField mysqlField(String sourceType) {
        TapField source = new TapField("number_value", sourceType);
        new TableFieldTypesGeneratorImpl().autoFill(source, mysqlTypes);
        assertTrue(source.getTapType() instanceof TapNumber, sourceType);
        return source;
    }

    private TapField assertMapping(TapField source, String expectedType) {
        LinkedHashMap<String, TapField> fields = new LinkedHashMap<>();
        fields.put(source.getName(), source);
        TapResult<LinkedHashMap<String, TapField>> result =
                new TargetTypesGeneratorImpl().convert(fields, paimonTypes, codecs);
        TapField target = result.getData().get(source.getName());
        assertEquals(expectedType, target.getDataType(), source.getDataType());
        assertTrue(
                result.getResultItems() == null || result.getResultItems().isEmpty(),
                "Mapping must not report precision loss");
        return target;
    }

    private static DefaultExpressionMatchingMap typeMap(String resource) throws Exception {
        try (InputStream input =
                PaimonNumericTypeMappingTest.class.getClassLoader().getResourceAsStream(resource)) {
            assertNotNull(input, resource);
            ObjectMapper mapper = new ObjectMapper();
            LinkedHashMap<String, DataMap> types =
                    mapper.convertValue(
                            mapper.readTree(input).get("dataTypes"),
                            new TypeReference<LinkedHashMap<String, DataMap>>() {});
            return DefaultExpressionMatchingMap.map(types);
        }
    }
}
