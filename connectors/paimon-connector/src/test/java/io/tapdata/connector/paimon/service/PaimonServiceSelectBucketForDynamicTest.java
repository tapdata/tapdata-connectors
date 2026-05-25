package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonServiceSelectBucketForDynamicTest {

    private static final String TABLE_ID = "table-id-1";
    private static final String TABLE_NAME = "test_table";
    private static final String PRIMARY_KEY_FIELD = "id";
    private static final String PRIMARY_KEY_VALUE = "1F3B8A2F-05A6-BD3E-D4A4-3A20E51A78CE";

    @Test
    void selectBucketForDynamicShouldReturnConsistentBucketForSamePrimaryKey() throws Exception {
        PaimonConfig config = new PaimonConfig();
        config.setBucketCount(4);
        PaimonService service = new PaimonService(config, mock(Log.class));

        TapTable table = mock(TapTable.class);
        when(table.getId()).thenReturn(TABLE_ID);
        when(table.getName()).thenReturn(TABLE_NAME);

        Collection<String> primaryKeys = Arrays.asList(PRIMARY_KEY_FIELD);
        when(table.primaryKeys(true)).thenReturn(primaryKeys);

        LinkedHashMap<String, TapField> fields = new LinkedHashMap<>();
        fields.put(PRIMARY_KEY_FIELD, mock(TapField.class));
        when(table.getNameFieldMap()).thenReturn(fields);

        int expectedBucket = invokeSelectBucketForDynamic(service, buildRowWithPrimaryKey(PRIMARY_KEY_VALUE), table);

        for (int i = 0; i < 50; i++) {
            int actualBucket = invokeSelectBucketForDynamic(service, buildRowWithPrimaryKey(PRIMARY_KEY_VALUE), table);
            System.out.println("Test iteration " + i + ": expected bucket = " + expectedBucket + ", actual bucket = " + actualBucket);
            assertEquals(expectedBucket, actualBucket);
        }
    }

    private GenericRow buildRowWithPrimaryKey(String primaryKeyValue) {
        GenericRow row = new GenericRow(1);
        row.setField(0, BinaryString.fromString(primaryKeyValue));
        return row;
    }

    private int invokeSelectBucketForDynamic(PaimonService service, GenericRow row, TapTable table) throws Exception {
        Method method = PaimonService.class.getDeclaredMethod("selectBucketForDynamic", GenericRow.class, TapTable.class);
        method.setAccessible(true);
        return (int) method.invoke(service, row, table);
    }
}
