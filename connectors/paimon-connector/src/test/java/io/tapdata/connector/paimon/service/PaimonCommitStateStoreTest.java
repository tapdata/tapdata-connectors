package io.tapdata.connector.paimon.service;

import io.tapdata.entity.utils.cache.KVMap;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonCommitStateStoreTest {

    @Test
    void absentStateShouldAtomicallyPublishAndReuseWinner() throws Exception {
        StateMapFixture stateMap = new StateMapFixture();
        FileStoreTable table = tableWithoutSnapshot();

        PaimonCommitStateStore.Binding first =
                PaimonCommitStateStore.bind(stateMap.kvMap, "file:///warehouse", table);
        PaimonCommitStateStore.Binding second =
                PaimonCommitStateStore.bind(stateMap.kvMap, "file:///warehouse", table);

        assertEquals(first.commitUser(), second.commitUser());
        assertEquals(0L, first.nextCommitIdentifier());
        assertEquals(1, stateMap.values.size());
        assertNotNull(stateMap.values.get(first.stateKey()));
    }

    @Test
    void snapshotShouldReconcileAndPersistIdentifierBeforeContextCreation() throws Exception {
        StateMapFixture stateMap = new StateMapFixture();
        FileStoreTable table = tableWithSnapshot("stable-user", 5L);
        String key = stateKey(table);
        stateMap.values.put(
                key,
                "{\"version\":1,\"commitUser\":\"stable-user\",\"nextCommitIdentifier\":3}");

        PaimonCommitStateStore.Binding binding =
                PaimonCommitStateStore.bind(stateMap.kvMap, "file:///warehouse", table);

        assertEquals("stable-user", binding.commitUser());
        assertEquals(6L, binding.nextCommitIdentifier());
        assertEquals(6L, PaimonCommitStateStore.parse(stateMap.values.get(key)).nextCommitIdentifier());
    }

    @Test
    void storedIdentifierAheadOfSnapshotShouldRemainMonotonic() throws Exception {
        StateMapFixture stateMap = new StateMapFixture();
        FileStoreTable table = tableWithSnapshot("stable-user", 5L);
        String key = stateKey(table);
        stateMap.values.put(
                key,
                "{\"version\":1,\"commitUser\":\"stable-user\",\"nextCommitIdentifier\":7}");

        PaimonCommitStateStore.Binding binding =
                PaimonCommitStateStore.bind(stateMap.kvMap, "file:///warehouse", table);

        assertEquals(7L, binding.nextCommitIdentifier());
    }

    @Test
    void saveShouldRejectChangedCommitUserWithoutOverwriting() throws Exception {
        StateMapFixture stateMap = new StateMapFixture();
        FileStoreTable table = tableWithoutSnapshot();
        PaimonCommitStateStore.Binding binding =
                PaimonCommitStateStore.bind(stateMap.kvMap, "file:///warehouse", table);
        stateMap.values.put(
                binding.stateKey(),
                "{\"version\":1,\"commitUser\":\"other-user\",\"nextCommitIdentifier\":0}");

        assertThrows(IllegalStateException.class, () -> binding.store().save(1L));
        assertEquals(
                "other-user",
                PaimonCommitStateStore.parse(stateMap.values.get(binding.stateKey())).commitUser());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "not-json",
            "[]",
            "{\"version\":2,\"commitUser\":\"u\",\"nextCommitIdentifier\":0}",
            "{\"version\":1,\"commitUser\":\"\",\"nextCommitIdentifier\":0}",
            "{\"version\":1,\"commitUser\":\"u\",\"nextCommitIdentifier\":-1}",
            "{\"version\":1,\"commitUser\":\"u\",\"nextCommitIdentifier\":1.5}",
            "{\"version\":1,\"commitUser\":\"u\",\"nextCommitIdentifier\":0,\"extra\":true}"
    })
    void malformedOrUnknownStateShouldFailFast(String json) {
        assertThrows(IllegalStateException.class, () -> PaimonCommitStateStore.parse(json));
    }

    @Test
    void maximumSnapshotIdentifierShouldFailWithoutOverflow() throws Exception {
        StateMapFixture stateMap = new StateMapFixture();
        FileStoreTable table = tableWithSnapshot("stable-user", Long.MAX_VALUE);
        stateMap.values.put(
                stateKey(table),
                "{\"version\":1,\"commitUser\":\"stable-user\",\"nextCommitIdentifier\":0}");

        assertThrows(
                IllegalStateException.class,
                () -> PaimonCommitStateStore.bind(stateMap.kvMap, "file:///warehouse", table));
    }

    @Test
    void equivalentUrisShouldProduceSameCredentialFreeStateKey() {
        String withSecrets = PaimonCommitStateStore.stateKey(
                "S3://user:password@Bucket/a/../warehouse/?token=secret#fragment",
                "S3://other:credential@Bucket/tables/../table/?secret=value");
        String canonical = PaimonCommitStateStore.stateKey(
                "s3://bucket/warehouse",
                "s3://bucket/table");

        assertEquals(canonical, withSecrets);
        org.junit.jupiter.api.Assertions.assertFalse(withSecrets.contains("secret"));
        org.junit.jupiter.api.Assertions.assertFalse(withSecrets.contains("password"));
    }

    @Test
    void unescapedLocalPathShouldMatchEncodedUri() {
        assertEquals(
                PaimonCommitStateStore.stateKey(
                        "file:///tmp/My%20Warehouse/%E6%B9%96", "file:///tmp/table"),
                PaimonCommitStateStore.stateKey(
                        "file:///tmp/My Warehouse/湖", "file:///tmp/table"));
    }

    private static FileStoreTable tableWithoutSnapshot() throws Exception {
        FileStoreTable table = mock(FileStoreTable.class);
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        when(table.location()).thenReturn(new Path("file:///table"));
        when(table.snapshotManager()).thenReturn(snapshotManager);
        when(snapshotManager.latestSnapshotOfUserFromFilesystem(anyString()))
                .thenReturn(Optional.empty());
        return table;
    }

    private static FileStoreTable tableWithSnapshot(String user, long identifier) throws Exception {
        FileStoreTable table = mock(FileStoreTable.class);
        SnapshotManager snapshotManager = mock(SnapshotManager.class);
        Snapshot snapshot = mock(Snapshot.class);
        when(table.location()).thenReturn(new Path("file:///table"));
        when(table.snapshotManager()).thenReturn(snapshotManager);
        when(snapshot.commitIdentifier()).thenReturn(identifier);
        when(snapshotManager.latestSnapshotOfUserFromFilesystem(user))
                .thenReturn(Optional.of(snapshot));
        return table;
    }

    private static String stateKey(FileStoreTable table) {
        return PaimonCommitStateStore.stateKey(
                "file:///warehouse", table.location().toUri().toString());
    }

    @SuppressWarnings("unchecked")
    private static final class StateMapFixture {
        private final Map<String, Object> values = new ConcurrentHashMap<>();
        private final KVMap<Object> kvMap = mock(KVMap.class);

        private StateMapFixture() {
            when(kvMap.get(anyString())).thenAnswer(invocation -> values.get(invocation.getArgument(0)));
            when(kvMap.putIfAbsent(anyString(), org.mockito.ArgumentMatchers.any()))
                    .thenAnswer(invocation ->
                            values.putIfAbsent(invocation.getArgument(0), invocation.getArgument(1)));
            doAnswer(invocation -> {
                values.put(invocation.getArgument(0), invocation.getArgument(1));
                return null;
            }).when(kvMap).put(anyString(), org.mockito.ArgumentMatchers.any());
        }
    }
}
