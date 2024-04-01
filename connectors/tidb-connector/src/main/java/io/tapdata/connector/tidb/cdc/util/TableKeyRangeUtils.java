package io.tapdata.connector.tidb.cdc.util;

import org.apache.flink.util.Preconditions;
import org.tikv.common.key.RowKey;
import org.tikv.common.util.KeyRangeUtils;
import org.tikv.kvproto.Coprocessor;
import org.tikv.shade.com.google.common.collect.ImmutableList;

import java.math.BigInteger;
import java.util.List;

public class TableKeyRangeUtils {
    public static Coprocessor.KeyRange getTableKeyRange(final long tableId) {
        return KeyRangeUtils.makeCoprocRange(
                RowKey.createMin(tableId).toByteString(),
                RowKey.createBeyondMax(tableId).toByteString());
    }

    public static List<Coprocessor.KeyRange> getTableKeyRanges(final long tableId, final int num) {
        Preconditions.checkArgument(num > 0, "Illegal value of num");

        if (num == 1) {
            return ImmutableList.of(getTableKeyRange(tableId));
        }

        final long delta =
                BigInteger.valueOf(Long.MAX_VALUE)
                        .subtract(BigInteger.valueOf(Long.MIN_VALUE + 1))
                        .divide(BigInteger.valueOf(num))
                        .longValueExact();
        final ImmutableList.Builder<Coprocessor.KeyRange> builder = ImmutableList.builder();
        for (int i = 0; i < num; i++) {
            final RowKey startKey =
                    (i == 0)
                            ? RowKey.createMin(tableId)
                            : RowKey.toRowKey(tableId, Long.MIN_VALUE + delta * i);
            final RowKey endKey =
                    (i == num - 1)
                            ? RowKey.createBeyondMax(tableId)
                            : RowKey.toRowKey(tableId, Long.MIN_VALUE + delta * (i + 1));
            builder.add(
                    KeyRangeUtils.makeCoprocRange(startKey.toByteString(), endKey.toByteString()));
        }
        return builder.build();
    }

    public static Coprocessor.KeyRange getTableKeyRange(final long tableId, final int num, final int idx) {
        Preconditions.checkArgument(idx >= 0 && idx < num, "Illegal value of idx");
        return getTableKeyRanges(tableId, num).get(idx);
    }

    public static boolean isRecordKey(final byte[] key) {
        return key[9] == '_' && key[10] == 'r';
    }
}
