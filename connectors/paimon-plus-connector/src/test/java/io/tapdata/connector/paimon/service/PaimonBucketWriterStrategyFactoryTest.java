package io.tapdata.connector.paimon.service;

import org.apache.paimon.crosspartition.GlobalIndexAssigner;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.index.BucketAssigner;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonBucketWriterStrategyFactoryTest {

    @Test
    void supportedModesMustExactlyMatchDependencyEnumAndBeImmutable() {
        Set<BucketMode> modes = PaimonBucketWriterStrategyFactory.supportedModes();

        assertEquals(EnumSet.allOf(BucketMode.class), modes);
        assertThrows(UnsupportedOperationException.class, () -> modes.remove(BucketMode.HASH_FIXED));
    }

    @Test
    void allModesMustMapToDedicatedConcreteStrategies() throws Exception {
        Fixture fixture = new Fixture();

        assertInstanceOf(
                HashFixedBucketWriterStrategy.class, fixture.create(BucketMode.HASH_FIXED));
        assertInstanceOf(
                HashDynamicBucketWriterStrategy.class, fixture.create(BucketMode.HASH_DYNAMIC));
        assertInstanceOf(
                KeyDynamicBucketWriterStrategy.class, fixture.create(BucketMode.KEY_DYNAMIC));
        assertInstanceOf(
                PostponeBucketWriterStrategy.class, fixture.create(BucketMode.POSTPONE_MODE));
        assertInstanceOf(
                BucketUnawareWriterStrategy.class, fixture.create(BucketMode.BUCKET_UNAWARE));
    }

    @Test
    void onlyKeyDynamicMustRequireIoManager() {
        for (BucketMode mode : BucketMode.values()) {
            assertEquals(
                    mode == BucketMode.KEY_DYNAMIC,
                    PaimonBucketWriterStrategyFactory.requiresIoManager(mode));
        }
    }

    @Test
    void onlyDynamicModesMustRequireOrderedSingleWriterIngress() {
        for (BucketMode mode : BucketMode.values()) {
            boolean expected =
                    mode == BucketMode.HASH_DYNAMIC || mode == BucketMode.KEY_DYNAMIC;
            assertEquals(
                    expected,
                    PaimonBucketWriterStrategyFactory.requiresOrderedSingleWriterIngress(mode));
        }
        assertTrue(
                PaimonBucketWriterStrategyFactory.requiresOrderedSingleWriterIngress(
                        BucketMode.HASH_DYNAMIC));
        assertFalse(
                PaimonBucketWriterStrategyFactory.requiresOrderedSingleWriterIngress(
                        BucketMode.HASH_FIXED));
    }

    private static final class Fixture {
        private final PaimonBucketWriterRuntimeFactory runtime =
                mock(PaimonBucketWriterRuntimeFactory.class);
        private final BucketAssigner hashAssigner = mock(BucketAssigner.class);
        private final GlobalIndexAssigner globalAssigner = mock(GlobalIndexAssigner.class);
        private final RecordReader<InternalRow> reader = mock(RecordReader.class);

        private Fixture() throws Exception {
            when(runtime.createHashBucketAssigner(any(), any())).thenReturn(hashAssigner);
            when(runtime.createGlobalIndexAssigner(any())).thenReturn(globalAssigner);
            when(runtime.createIndexBootstrapReader(any())).thenReturn(reader);
            when(reader.readBatch()).thenReturn(null);
            doNothing().when(globalAssigner).open(any(Long.class), any(), any(Integer.class), any(Integer.class), any());
        }

        private PaimonBucketWriterStrategy create(BucketMode mode) throws Exception {
            FileStoreTable table = mock(FileStoreTable.class);
            StreamTableWrite writer = mock(StreamTableWrite.class);
            IOManager ioManager = mock(IOManager.class);
            when(table.bucketMode()).thenReturn(mode);
            if (mode == BucketMode.HASH_DYNAMIC || mode == BucketMode.KEY_DYNAMIC) {
                TableSchema schema = schema();
                when(table.primaryKeys()).thenReturn(Arrays.asList("id", "pt"));
                when(table.partitionKeys()).thenReturn(Collections.singletonList("pt"));
                when(table.schema()).thenReturn(schema);
                when(table.rowType()).thenReturn(schema.logicalRowType());
            } else {
                when(table.primaryKeys()).thenReturn(Collections.emptyList());
                when(table.partitionKeys()).thenReturn(Collections.emptyList());
            }
            SnapshotManager snapshotManager = mock(SnapshotManager.class);
            when(table.snapshotManager()).thenReturn(snapshotManager);
            when(snapshotManager.latestSnapshotIdFromFileSystem()).thenReturn(1L, 1L);
            return PaimonBucketWriterStrategyFactory.create(
                    new PaimonBucketWriterStrategyContext(
                            "default.t",
                            table,
                            writer,
                            "user",
                            ioManager,
                            PaimonWriteSemanticContractTestFactory.forMode(mode)),
                    runtime);
        }

        private static TableSchema schema() {
            java.util.List<DataField> fields =
                    Arrays.asList(
                            new DataField(0, "pt", new IntType()),
                            new DataField(1, "id", new IntType()));
            return new TableSchema(
                    0,
                    fields,
                    RowType.currentHighestFieldId(fields),
                    Collections.singletonList("pt"),
                    Arrays.asList("id", "pt"),
                    Collections.emptyMap(),
                    "");
        }
    }
}
