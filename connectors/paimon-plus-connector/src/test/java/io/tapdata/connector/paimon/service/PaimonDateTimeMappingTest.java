package io.tapdata.connector.paimon.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tapdata.connector.paimon.PaimonConnector;
import io.tapdata.connector.paimon.schema.PaimonDataTypeConverter;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.codec.filter.TapCodecsFilterManager;
import io.tapdata.entity.conversion.impl.TableFieldTypesGeneratorImpl;
import io.tapdata.entity.conversion.impl.TargetTypesGeneratorImpl;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.*;

class PaimonDateTimeMappingTest {
    @ParameterizedTest
    @ValueSource(strings = {"datetime", "datetime(3)", "datetime(6)"})
    void fullMysqlDateTimeRangeMustMapWithoutFalsePrecisionWarnings(String sourceType) throws Exception {
        TapField source = new TapField("v", sourceType);
        new TableFieldTypesGeneratorImpl().autoFill(source, types("fixtures/mysql-temporal-types.json"));
        LinkedHashMap<String, TapField> fields = new LinkedHashMap<>();
        fields.put("v", source);
        var result = new TargetTypesGeneratorImpl().convert(fields, types("spec.json"), codecs());
        assertEquals(sourceType.equals("datetime") ? "TIMESTAMP(0)" : sourceType.toUpperCase().replace("DATETIME", "TIMESTAMP"),
                result.getData().get("v").getDataType());
        assertTrue(result.getResultItems() == null || result.getResultItems().isEmpty(),
                "Native TIMESTAMP supports the MySQL DATETIME range");
    }

    @ParameterizedTest
    @ValueSource(strings = {"UTC", "Asia/Shanghai", "America/Los_Angeles"})
    void mysqlDateTimesMustKeepWallClockAndMicroseconds(String zone) throws Exception {
        TimeZone before = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(zone));
            TapField field = new TapField("v", "datetime(6)");
            new TableFieldTypesGeneratorImpl().autoFill(field, types("fixtures/mysql-temporal-types.json"));
            TapCodecsFilterManager codecs = codecs();
            for (String text : new String[] {"1000-01-01T00:00:00", "1969-12-31T23:59:59.123456",
                    "2039-01-01T00:00:00.123456", "9999-12-31T23:59:59.999999"}) {
                LocalDateTime expected = LocalDateTime.parse(text);
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("v", expected);
                codecs.transformToTapValueMap(row, Map.of("v", field));
                codecs.transformFromTapValueMap(row);
                Object internal = PaimonDataTypeConverter.toInternalValue("v", row.get("v"), DataTypes.TIMESTAMP(6));
                assertEquals(expected, new InternalRowSerializer(DataTypes.TIMESTAMP(6))
                        .toBinaryRow(GenericRow.of(internal)).getTimestamp(0, 6).toLocalDateTime(), zone + " " + text);
            }
        } finally {
            TimeZone.setDefault(before);
        }
    }

    private TapCodecsFilterManager codecs() {
        TapCodecsRegistry registry = new TapCodecsRegistry();
        new PaimonConnector().registerCapabilities(new ConnectorFunctions(), registry);
        return TapCodecsFilterManager.create(registry);
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
