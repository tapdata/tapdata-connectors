package io.tapdata.connector.paimon.schema;

import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

class PaimonDecimalExactScaleTest {
    @ParameterizedTest
    @CsvSource({"'1.235', 5, 2", "'-1.235', 5, 2", "'0.000001', 38, 0",
            "'999999999999999999999999999999999999999', 38, 0"})
    void valuesOutsideExactPhysicalDecimalEnvelopeMustFail(String value, int precision, int scale) {
        PaimonFatalWriteException failure = assertThrows(PaimonFatalWriteException.class,
                () -> PaimonDataTypeConverter.toInternalValue("amount", value, DataTypes.DECIMAL(precision, scale)));
        assertTrue(failure.getMessage().contains("amount"));
        assertTrue(failure.getMessage().contains("DECIMAL"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"1.2300", "0", "-999.99000"})
    void removingInsignificantZeroesMustRemainExact(String source) {
        Decimal converted = (Decimal) PaimonDataTypeConverter.toInternalValue("amount", source, DataTypes.DECIMAL(5, 2));
        assertEquals(0, new java.math.BigDecimal(source).compareTo(converted.toBigDecimal()));
    }
}
