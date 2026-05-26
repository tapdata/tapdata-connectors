package io.tapdata.perftest.core.client;

public class WriteResult {
    private final long insertedCount;
    private final long updatedCount;
    private final long deletedCount;
    private final long errorCount;
    private final long elapsedMs;

    public WriteResult(long inserted, long updated, long deleted, long errors, long elapsedMs) {
        this.insertedCount = inserted;
        this.updatedCount  = updated;
        this.deletedCount  = deleted;
        this.errorCount    = errors;
        this.elapsedMs     = elapsedMs;
    }

    public long getTotalWritten() { return insertedCount + updatedCount + deletedCount; }
    public long getInsertedCount() { return insertedCount; }
    public long getUpdatedCount()  { return updatedCount; }
    public long getDeletedCount()  { return deletedCount; }
    public long getErrorCount()    { return errorCount; }
    public long getElapsedMs()     { return elapsedMs; }

    @Override
    public String toString() {
        return String.format("WriteResult{inserted=%d, updated=%d, deleted=%d, errors=%d, elapsedMs=%d}",
            insertedCount, updatedCount, deletedCount, errorCount, elapsedMs);
    }
}
