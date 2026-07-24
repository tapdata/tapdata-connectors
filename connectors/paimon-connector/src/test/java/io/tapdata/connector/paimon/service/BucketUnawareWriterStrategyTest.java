package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BucketUnawareWriterStrategyTest {

    @ParameterizedTest
    @EnumSource(RowKind.class)
    void allRowKindsMustDelegateNativeWriteWithSameRow(RowKind rowKind) throws Exception {
        Fixture fixture = new Fixture();
        InternalRow row = GenericRow.ofKind(rowKind, 1);

        fixture.strategy.write(row);

        verify(fixture.writer).write(org.mockito.ArgumentMatchers.same(row));
        verify(fixture.writer, never()).write(any(InternalRow.class), anyInt());
    }

    @Test
    void duplicateAppendRowsMustBothReachDelegate() throws Exception {
        Fixture fixture = new Fixture();
        InternalRow row = GenericRow.of(1);

        fixture.strategy.write(row);
        fixture.strategy.write(row);

        verify(fixture.writer, times(2)).write(org.mockito.ArgumentMatchers.same(row));
    }

    @Test
    void primaryKeyTableMustFailConstruction() {
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.bucketMode()).thenReturn(BucketMode.BUCKET_UNAWARE);
        when(table.primaryKeys()).thenReturn(Collections.singletonList("id"));
        PaimonBucketWriterStrategyContext context =
                new PaimonBucketWriterStrategyContext(
                        "default.t",
                        table,
                        mock(StreamTableWrite.class),
                        "user",
                        null,
                        PaimonWriteSemanticContractTestFactory.forMode(
                                BucketMode.BUCKET_UNAWARE));

        assertThrows(IllegalArgumentException.class, () -> new BucketUnawareWriterStrategy(context));
    }

    private static final class Fixture {
        private final FileStoreTable table = mock(FileStoreTable.class);
        private final StreamTableWrite writer = mock(StreamTableWrite.class);
        private final BucketUnawareWriterStrategy strategy;

        private Fixture() {
            when(table.bucketMode()).thenReturn(BucketMode.BUCKET_UNAWARE);
            when(table.primaryKeys()).thenReturn(Collections.emptyList());
            strategy =
                    new BucketUnawareWriterStrategy(
                            new PaimonBucketWriterStrategyContext(
                                    "default.t",
                                    table,
                                    writer,
                                    "user",
                                    null,
                                    PaimonWriteSemanticContractTestFactory.forMode(
                                            BucketMode.BUCKET_UNAWARE)));
        }
    }
}
