package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonServicePrimaryKeyChangeTest {

    private PaimonService service;
    private TapTable table;

    @BeforeEach
    void setUp() throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        service = new PaimonService(config, mock(Log.class));
        table = mock(TapTable.class);
        when(table.getName()).thenReturn("t");
        when(table.primaryKeys(true)).thenReturn(Arrays.asList("a", "b"));

        // Deliberately use a target RowType order different from the primary-key/TapTable order.
        fieldCache(service).put(
                "default.t",
                Arrays.asList(
                        new DataField(0, "b", DataTypes.STRING()),
                        new DataField(1, "value", DataTypes.STRING()),
                        new DataField(2, "a", DataTypes.STRING())));
    }

    @Test
    void nonKeyValueChangeMustNotBeMistakenForPrimaryKeyChangeWhenMapOrderDiffers() {
        GenericRow before = GenericRow.of(
                string("b"), string("old"), string("a"));
        GenericRow after = GenericRow.of(
                string("b"), string("new"), string("a"));

        assertFalse(service.isPrimaryKeyChanged(before, after, table));
    }

    @Test
    void compositeKeysMustBeComparedByTypedFieldsWithoutDelimiterCollision() {
        // Old string concatenation produced "a|b|c|" for both key tuples:
        // before=(a="a|b", b="c"), after=(a="a", b="b|c").
        GenericRow before = GenericRow.of(
                string("c"), string("same"), string("a|b"));
        GenericRow after = GenericRow.of(
                string("b|c"), string("same"), string("a"));

        assertTrue(service.isPrimaryKeyChanged(before, after, table));
    }

    @Test
    void missingPrimaryKeyInTargetRowTypeMustFailInsteadOfBeingIgnored() throws Exception {
        fieldCache(service).put(
                "default.t",
                Collections.singletonList(new DataField(0, "a", DataTypes.STRING())));
        GenericRow row = GenericRow.of(string("a"));

        org.junit.jupiter.api.Assertions.assertThrows(
                PaimonFatalWriteException.class,
                () -> service.isPrimaryKeyChanged(row, row, table));
    }

    private static BinaryString string(String value) {
        return BinaryString.fromString(value);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, List<DataField>> fieldCache(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("paimonFieldCache");
        field.setAccessible(true);
        return (Map<String, List<DataField>>) field.get(service);
    }
}
