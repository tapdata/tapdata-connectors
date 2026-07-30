package io.tapdata.connector.paimon.service;

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PostponeBucketWriterStrategyTest {

    @Test
    void mustDelegateNativeWriteWithoutComputingBucket() throws Exception {
        Fixture fixture = new Fixture();
        InternalRow row = GenericRow.of(1);

        fixture.strategy.write(row);

        verify(fixture.writer).write(org.mockito.ArgumentMatchers.same(row));
        verify(fixture.writer, never()).write(any(InternalRow.class), anyInt());
    }

    @Test
    void mustNotTriggerCompactOrRescale() throws Exception {
        Fixture fixture = new Fixture();

        fixture.strategy.write(GenericRow.of(1));

        verify(fixture.writer, never()).compact(any(), anyInt(), org.mockito.ArgumentMatchers.anyBoolean());
    }

    private static final class Fixture {
        private final FileStoreTable table = mock(FileStoreTable.class);
        private final StreamTableWrite writer = mock(StreamTableWrite.class);
        private final PostponeBucketWriterStrategy strategy;

        private Fixture() {
            when(table.bucketMode()).thenReturn(BucketMode.POSTPONE_MODE);
            strategy =
                    new PostponeBucketWriterStrategy(
                            new PaimonBucketWriterStrategyContext(
                                    "default.t",
                                    table,
                                    writer,
                                    "user",
                                    null,
                                    PaimonWriteSemanticContractTestFactory.forMode(
                                            BucketMode.POSTPONE_MODE)));
        }
    }
}
