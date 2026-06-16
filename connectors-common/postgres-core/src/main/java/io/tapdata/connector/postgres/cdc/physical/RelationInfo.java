package io.tapdata.connector.postgres.cdc.physical;

import java.util.List;

/**
 * Resolved metadata for a relation identified by its {@code relNumber} in WAL:
 * the schema-qualified name plus the physically-ordered column list (including
 * dropped columns, which still occupy tuple slots) and the names of the columns
 * that form the replica identity / primary key.
 *
 * @author Jarad
 */
public class RelationInfo {

    public final String schema;
    public final String table;
    /** ordered by physical attnum, includes dropped columns. */
    public final List<ColumnInfo> columns;
    /** replica-identity (or primary key) column names, may be empty. */
    public final List<String> keyColumns;
    /** true when the table is configured with REPLICA IDENTITY FULL, which
     *  makes the WAL self-contained for UPDATE/DELETE old tuples and lets the
     *  physical miner bypass the page-state cache for this relation. */
    public final boolean replicaIdentityFull;

    public RelationInfo(String schema, String table, List<ColumnInfo> columns, List<String> keyColumns,
                        boolean replicaIdentityFull) {
        this.schema = schema;
        this.table = table;
        this.columns = columns;
        this.keyColumns = keyColumns;
        this.replicaIdentityFull = replicaIdentityFull;
    }
}
