package io.tapdata.connector.paimon.service;

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.StreamTableWrite;

import java.util.Objects;

/** Immutable construction context shared by all bucket writer strategies. */
final class PaimonBucketWriterStrategyContext {

    private final String tableKey;
    private final FileStoreTable table;
    private final StreamTableWrite writer;
    private final String commitUser;
    private final IOManager ioManager;
    private final PaimonWriteSemanticContract writeSemanticContract;

    PaimonBucketWriterStrategyContext(
            String tableKey,
            FileStoreTable table,
            StreamTableWrite writer,
            String commitUser,
            IOManager ioManager,
            PaimonWriteSemanticContract writeSemanticContract) {
        this.tableKey = Objects.requireNonNull(tableKey, "tableKey");
        this.table = Objects.requireNonNull(table, "table");
        this.writer = Objects.requireNonNull(writer, "writer");
        this.commitUser = Objects.requireNonNull(commitUser, "commitUser");
        this.ioManager = ioManager;
        this.writeSemanticContract =
                Objects.requireNonNull(writeSemanticContract, "writeSemanticContract");
    }

    String tableKey() {
        return tableKey;
    }

    FileStoreTable table() {
        return table;
    }

    StreamTableWrite writer() {
        return writer;
    }

    String commitUser() {
        return commitUser;
    }

    IOManager ioManager() {
        return ioManager;
    }

    PaimonWriteSemanticContract writeSemanticContract() {
        return writeSemanticContract;
    }
}
