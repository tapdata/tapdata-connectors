package io.tapdata.connector.postgres.partition.wrappper;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.TapPartitionField;
import io.tapdata.entity.schema.partition.type.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;

import static io.tapdata.connector.postgres.partition.TableType.*;
import static org.mockito.Mockito.mock;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/10/24 12:13
 */
public class PGPartitionWrapperTest {

    @Test
    void testHashWrapper() {
        PGPartitionWrapper hashWrap = PGPartitionWrapper.instance(HASH);
        TapTable table = new TapTable();
        table.setId("test");

        Log log = mock(Log.class);

        Assertions.assertThrows(CoreException.class, () -> {
            hashWrap.parse(table, "", "", log);
        });

        Assertions.assertDoesNotThrow(() -> {
            List<TapPartitionType> result = hashWrap.parse(table, "FOR VALUES WITH (MODULUS 4, REMAINDER 0)", null, log);
            Assertions.assertNotNull(result);
            Assertions.assertEquals(1, result.size());
            Assertions.assertInstanceOf(TapPartitionHash.class, result.get(0));
            Assertions.assertEquals(4, ((TapPartitionHash)result.get(0)).getModulus());
        });
    }

    @Test
    void testListWrapper() {
        PGPartitionWrapper listWrap = PGPartitionWrapper.instance(LIST);
        TapTable table = new TapTable();
        table.setId("test");

        Log log = mock(Log.class);

        Assertions.assertThrows(CoreException.class, () -> {
            listWrap.parse(table, "", null, log);
        });

        Assertions.assertDoesNotThrow(() -> {
            List<TapPartitionType> result = listWrap.parse(table, "DEFAULT", null, log);
            Assertions.assertNotNull(result);

            result = listWrap.parse(table, "('USA', 'UK')", null, log);
            Assertions.assertNotNull(result);
            Assertions.assertEquals(1, result.size());
            Assertions.assertEquals(2, ((TapPartitionList)result.get(0)).getListIn().size());
        });
    }

    @Test
    void testRangeWrapper() {
        PGPartitionWrapper rangeWrap = PGPartitionWrapper.instance(RANGE);

        Assertions.assertEquals(TapPartitionStage.RANGE, rangeWrap.type());

        TapTable table = new TapTable();
        table.setId("test");

        Log log = mock(Log.class);
        Assertions.assertDoesNotThrow(() -> {
            List<TapPartitionType> result =
                    rangeWrap.parse(table, "FOR VALUES FROM ('2024-01-01 00:00:00+08') TO ('2024-02-01 00:00:00+08')", null, log);
            Assertions.assertNotNull(result);
            Assertions.assertEquals(1, result.size());
        });

        List<TapPartitionType> result =
                rangeWrap.parse(table, "FOR VALUES FROM ('1') TO ('2000')", null, log);
        Assertions.assertNotNull(result);

        result =
                rangeWrap.parse(table, "test", null, log);
        Assertions.assertNull(result);

        result =
                rangeWrap.parse(table, "FOR VALUES FROM ('1','2') TO ('2000')", null, log);
        Assertions.assertNull(result);

        TapRangeValue.ValueType valueType = ((RangeWrapper) rangeWrap).valueType(RangeWrapper.MAXVALUE);
        Assertions.assertEquals(valueType, TapRangeValue.ValueType.MAX);

        valueType = ((RangeWrapper) rangeWrap).valueType(RangeWrapper.MINVALUE);
        Assertions.assertEquals(valueType, TapRangeValue.ValueType.MIN);

        valueType = ((RangeWrapper) rangeWrap).valueType("");
        Assertions.assertEquals(valueType, TapRangeValue.ValueType.NORMAL);
    }

    @Test
    void testInheritWrapper() {
        PGPartitionWrapper inheritWrap = PGPartitionWrapper.instance(INHERIT);

        Assertions.assertEquals(TapPartitionStage.INHERIT, inheritWrap.type());

        List<TapPartitionType> result = inheritWrap.parse(null, null, null, null);
        Assertions.assertNull(result);
    }

    @Test
    void testPartitionFields() {
        Log log = mock(Log.class);

        Assertions.assertDoesNotThrow(() -> {
            List<TapPartitionField> result = PGPartitionWrapper.partitionFields(null, "test", null, "test", log);
            Assertions.assertNull(result);
        });

        TapTable table = new TapTable();

        table.setNameFieldMap(new LinkedHashMap<>());

        Assertions.assertThrows(CoreException.class, () -> {
            List<TapPartitionField> result = PGPartitionWrapper.partitionFields(table, RANGE, "", "test", log);
            Assertions.assertNotNull(result);
        });
        Assertions.assertThrows(CoreException.class, () -> {
            List<TapPartitionField> result = PGPartitionWrapper.partitionFields(table, RANGE, "test", "test", log);
            Assertions.assertNotNull(result);
        });
    }

    @Test
    void testType() {
        Log log = mock(Log.class);
        TapPartitionStage result = PGPartitionWrapper.type("", "test", log);
        Assertions.assertNull(result);
    }

}
