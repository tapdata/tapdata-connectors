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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PaimonFloatMappingTest {
    @ParameterizedTest
    @ValueSource(strings = {"float", "float(8,2)", "float(8,2) unsigned",
            "float(20,6)", "float(20,6) unsigned", "float(30,30)", "float(8,0)"})
    void currentFloatDeclarationsMustMapToDoubleAndPreserveSourceValues(String declaration) throws Exception {
        TapField source = new TapField("v", declaration);
        new TableFieldTypesGeneratorImpl().autoFill(source, types("fixtures/mysql-floating-types.json"));
        TapCodecsRegistry registry = new TapCodecsRegistry();
        new PaimonConnector().registerCapabilities(new ConnectorFunctions(), registry);
        TapCodecsFilterManager codecs = TapCodecsFilterManager.create(registry);
        TapField target = new TargetTypesGeneratorImpl().convert(
                new LinkedHashMap<>(Map.of("v", source)), types("spec.json"), codecs).getData().get("v");
        assertEquals(DataTypes.DOUBLE(), PaimonDataTypeConverter.toPaimonDataType(target));
        float value = declaration.contains(",0)") ? 123F : 0.125F;
        for (float input : new float[] {0F, value, declaration.endsWith("unsigned") ? value : -value}) {
            Map<String, Object> row = new LinkedHashMap<>(Map.of("v", input));
            codecs.transformToTapValueMap(row, Map.of("v", source));
            codecs.transformFromTapValueMap(row);
            Object internal = PaimonDataTypeConverter.toInternalValue("v", row.get("v"), DataTypes.DOUBLE());
            double actual = new InternalRowSerializer(DataTypes.DOUBLE())
                    .toBinaryRow(GenericRow.of(internal)).getDouble(0);
            assertEquals(Double.doubleToLongBits(input), Double.doubleToLongBits(actual));
        }
    }

    @Test
    void nativeFloatMustRemainExplicitlyAvailable() throws Exception {
        TapField field = new TapField("v", "FLOAT");
        new TableFieldTypesGeneratorImpl().autoFill(field, types("spec.json"));
        assertNotNull(field.getTapType());
        assertEquals(DataTypes.FLOAT(), PaimonDataTypeConverter.toPaimonDataType(field));
        assertEquals(1.25F, PaimonDataTypeConverter.toInternalValue("v", 1.25F, DataTypes.FLOAT()));
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
