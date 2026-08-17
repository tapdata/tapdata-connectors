package io.tapdata.connector.paimon.service;

import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Verifies the RocksDB ABI contract shared by Tapdata Engine and Paimon KEY_DYNAMIC writers. */
class PaimonRocksDbRuntimeContractTest {

    @Test
    void keyDynamicWriterMustUseTapdataPlatformRocksDbAbi() {
        RocksDB.Version version = RocksDB.rocksdbVersion();

        assertEquals(7, version.getMajor());
        assertEquals(3, version.getMinor());
        assertEquals(1, version.getPatch());
    }
}
