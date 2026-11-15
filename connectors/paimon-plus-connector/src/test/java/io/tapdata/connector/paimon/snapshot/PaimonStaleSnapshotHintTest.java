package io.tapdata.connector.paimon.snapshot;

import io.tapdata.connector.paimon.commit.PaimonCommitStateStore;
import io.tapdata.entity.utils.cache.KVMap;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.HintFileUtils;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class PaimonStaleSnapshotHintTest {
    @TempDir java.nio.file.Path temp;

    @Test
    void orphanLatestShouldBehaveLikeAnEmptyTableWithoutDeletingEvidence() throws Exception {
        java.nio.file.Path dir = Files.createDirectories(temp.resolve("snapshot"));
        Files.write(dir.resolve("LATEST"), "7".getBytes(UTF_8));
        SnapshotManager manager = new SnapshotManager(LocalFileIO.create(), new Path(temp.toUri()),
                null, null, null);
        assertNull(manager.latestSnapshotIdFromFileSystem());
        assertNull(manager.earliestSnapshotId());
        assertFalse(manager.latestSnapshotOfUserFromFilesystem("user").isPresent());
        assertEquals("7", Files.readString(dir.resolve("LATEST")));
    }

    @Test
    void missingHintTargetShouldDiscoverActualSnapshotsEvenWhenHintIsAhead() throws Exception {
        java.nio.file.Path dir = Files.createDirectories(temp.resolve("snapshot"));
        Files.write(dir.resolve("LATEST"), "99".getBytes(UTF_8));
        Files.write(dir.resolve("snapshot-1"), new byte[0]);
        Files.write(dir.resolve("snapshot-3"), new byte[0]);
        assertEquals(Long.valueOf(3), latest(LocalFileIO.create(), new Path(dir.toUri())));
    }

    @Test
    void validLatestShouldKeepFastPathWithoutListing() throws Exception {
        FileIO io = mock(FileIO.class);
        Path dir = new Path("file:///table/snapshot");
        when(io.readOverwrittenFileUtf8(new Path(dir, "LATEST")))
                .thenReturn(java.util.Optional.of("3"));
        when(io.exists(new Path(dir, "snapshot-3"))).thenReturn(true);
        assertEquals(Long.valueOf(3), latest(io, dir));
        verify(io, never()).listStatus(any(Path.class));
    }

    @Test
    void behindHintShouldStillDiscoverNewerSnapshot() throws Exception {
        java.nio.file.Path dir = Files.createDirectories(temp.resolve("snapshot"));
        Files.write(dir.resolve("LATEST"), "1".getBytes(UTF_8));
        Files.write(dir.resolve("snapshot-1"), new byte[0]);
        Files.write(dir.resolve("snapshot-2"), new byte[0]);
        assertEquals(Long.valueOf(2), latest(LocalFileIO.create(), new Path(dir.toUri())));
    }

    @Test
    void accessFailureMustPropagateRatherThanPretendTableIsEmpty() throws Exception {
        FileIO io = mock(FileIO.class);
        Path dir = new Path("file:///table/snapshot");
        when(io.readOverwrittenFileUtf8(new Path(dir, "LATEST")))
                .thenReturn(java.util.Optional.of("3"));
        IOException denied = new IOException("access denied");
        when(io.exists(new Path(dir, "snapshot-3"))).thenThrow(denied);
        assertSame(denied, assertThrows(IOException.class, () -> latest(io, dir)));
    }

    @Test
    void dropRecreateWithOrphanHintShouldBindCommitReadAndRecover() throws Exception {
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(new Path(temp.toUri())))) {
            catalog.createDatabase("db", false);
            Identifier id = Identifier.create("db", "same_name");
            Schema schema = Schema.newBuilder().column("id", DataTypes.INT().notNull())
                    .primaryKey("id").option("bucket", "1").option("file.format", "avro").build();
            catalog.createTable(id, schema, false);
            FileStoreTable old = (FileStoreTable) catalog.getTable(id);
            write(old, "old-user", 0, 100);
            Path location = old.location();
            catalog.dropTable(id, false);
            // Model partial physical deletion: only the old LATEST hint remains.
            java.nio.file.Path snapshotDir = Files.createDirectories(
                    java.nio.file.Paths.get(location.toUri()).resolve("snapshot"));
            Files.write(snapshotDir.resolve("LATEST"), "9".getBytes(UTF_8));
            catalog.createTable(id, schema, false);
            FileStoreTable table = (FileStoreTable) catalog.getTable(id);
            Map<String, Object> values = new HashMap<>();
            @SuppressWarnings("unchecked") KVMap<Object> state = mock(KVMap.class);
            when(state.get(anyString())).thenAnswer(i -> values.get(i.getArgument(0)));
            when(state.putIfAbsent(anyString(), any())).thenAnswer(
                    i -> values.putIfAbsent(i.getArgument(0), i.getArgument(1)));
            doAnswer(i -> { values.put(i.getArgument(0), i.getArgument(1)); return null; })
                    .when(state).put(anyString(), any());
            PaimonCommitStateStore.Binding binding =
                    PaimonCommitStateStore.bind(state, temp.toUri().toString(), table);
            assertEquals(0L, binding.nextCommitIdentifier());
            write(table, binding.commitUser(), binding.nextCommitIdentifier(), 200);
            assertEquals(Long.valueOf(1), table.snapshotManager().latestSnapshotIdFromFileSystem());
            // Exercise native manifest discovery and actual data-file reading as well.
            org.apache.paimon.table.source.ReadBuilder read = table.newReadBuilder();
            try (org.apache.paimon.reader.RecordReader<org.apache.paimon.data.InternalRow> reader =
                         read.newRead().createReader(read.newScan().plan().splits())) {
                org.apache.paimon.reader.RecordReader.RecordIterator<org.apache.paimon.data.InternalRow>
                        batch = reader.readBatch();
                assertNotNull(batch);
                assertEquals(200, batch.next().getInt(0));
                assertNull(batch.next());
                batch.releaseBatch();
                assertNull(reader.readBatch());
            }
            // Simulate committed snapshot with task-state save missing: keep the same user,
            // recover the identifier from the retained snapshot and persist it before writing.
            PaimonCommitStateStore.Binding recovered = PaimonCommitStateStore.bind(
                    state, temp.toUri().toString(), (FileStoreTable) catalog.getTable(id));
            assertEquals(binding.commitUser(), recovered.commitUser());
            assertEquals(1L, recovered.nextCommitIdentifier());
            write(table, recovered.commitUser(), recovered.nextCommitIdentifier(), 201);
            assertEquals(Long.valueOf(2), table.snapshotManager().latestSnapshotIdFromFileSystem());
        }
    }

    private Long latest(FileIO io, Path dir) throws IOException {
        return HintFileUtils.findLatest(io, dir, "snapshot-", n -> new Path(dir, "snapshot-" + n));
    }

    private void write(FileStoreTable table, String user, long commitId, int value) throws Exception {
        StreamWriteBuilder builder = table.newStreamWriteBuilder().withCommitUser(user);
        try (StreamTableWrite writer = builder.newWrite(); StreamTableCommit commit = builder.newCommit()) {
            writer.write(GenericRow.of(value), 0);
            commit.commit(commitId, writer.prepareCommit(false, commitId));
        }
    }
}
