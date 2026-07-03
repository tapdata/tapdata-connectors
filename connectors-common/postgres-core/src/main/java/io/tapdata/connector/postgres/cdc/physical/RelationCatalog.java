package io.tapdata.connector.postgres.cdc.physical;

import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.entity.logger.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves the {@code relNumber} (relfilenode) carried in WAL block references
 * to a {@link RelationInfo} by querying the system catalogs, and caches the
 * result. Because a relfilenode is rewritten by TRUNCATE / VACUUM FULL / cluster
 * the cache can be invalidated wholesale via {@link #invalidate()}; misses are
 * remembered negatively to avoid hammering the catalog for system relations.
 *
 * @author Jarad
 */
public class RelationCatalog {

    private final PostgresJdbcContext jdbcContext;
    private final Log log;
    private final Map<Long, RelationInfo> cache = new ConcurrentHashMap<>();
    private final Map<Long, Boolean> negative = new ConcurrentHashMap<>();

    private static final String REL_BY_FILENODE =
            "SELECT c.oid, n.nspname, c.relname, c.relreplident FROM pg_class c " +
                    "JOIN pg_namespace n ON c.relnamespace = n.oid " +
                    "WHERE c.relfilenode = %d AND c.relkind IN ('r','m','p') LIMIT 1";
    private static final String COLUMNS =
            "SELECT attname, attnum, atttypid, attlen, attalign, attisdropped FROM pg_attribute " +
                    "WHERE attrelid = %d AND attnum > 0 ORDER BY attnum";
    private static final String KEYS =
            "SELECT a.attname FROM pg_index i " +
                    "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) " +
                    "WHERE i.indrelid = %d AND (i.indisprimary OR i.indisreplident)";

    public RelationCatalog(PostgresJdbcContext jdbcContext, Log log) {
        this.jdbcContext = jdbcContext;
        this.log = log;
    }

    public RelationInfo lookup(long relNumber) {
        if (relNumber <= 0) {
            return null;
        }
        RelationInfo cached = cache.get(relNumber);
        if (cached != null) {
            return cached;
        }
        if (negative.containsKey(relNumber)) {
            return null;
        }
        RelationInfo loaded = load(relNumber);
        if (loaded == null) {
            negative.put(relNumber, Boolean.TRUE);
        } else {
            cache.put(relNumber, loaded);
        }
        return loaded;
    }

    public void invalidate() {
        cache.clear();
        negative.clear();
    }

    /**
     * Apply a single pg_attribute change (INSERT/UPDATE/DELETE) to the cached
     * {@link RelationInfo} for the matching schema.table. Called from the consumer
     * thread when a monitored table's catalog row is decoded from WAL, so that
     * subsequent DML in the same transaction decodes with the correct column layout.
     *
     * @return true if a cached entry was found and updated, false if no entry was
     *         cached yet (caller should track the change as pending)
     */
    public boolean applyPgAttributeChange(String schema, String table, String op,
                                           int attnum, String attname, long atttypid,
                                           int attlen, char attalign, boolean attisdropped) {
        for (Map.Entry<Long, RelationInfo> entry : cache.entrySet()) {
            RelationInfo rel = entry.getValue();
            if (!rel.schema.equals(schema) || !rel.table.equals(table)) {
                continue;
            }
            List<ColumnInfo> newColumns = new ArrayList<>(rel.columns);
            switch (op) {
                case "INSERT": {
                    ColumnInfo col = new ColumnInfo(attname, attnum, atttypid, attlen, attalign, attisdropped);
                    int pos = 0;
                    while (pos < newColumns.size() && newColumns.get(pos).attnum < attnum) {
                        pos++;
                    }
                    newColumns.add(pos, col);
                    break;
                }
                case "UPDATE": {
                    for (int i = 0; i < newColumns.size(); i++) {
                        if (newColumns.get(i).attnum == attnum) {
                            newColumns.set(i, new ColumnInfo(attname, attnum, atttypid, attlen, attalign, attisdropped));
                            break;
                        }
                    }
                    break;
                }
                case "DELETE":
                    newColumns.removeIf(c -> c.attnum == attnum);
                    break;
                default:
                    return false;
            }
            RelationInfo updated = new RelationInfo(rel.schema, rel.table, newColumns,
                    new ArrayList<>(rel.keyColumns), rel.replicaIdentityFull);
            cache.put(entry.getKey(), updated);
            return true;
        }
        return false;
    }

    /**
     * Apply pending column changes to a freshly-loaded {@link RelationInfo} before
     * it is returned to the caller. Used when DDL was detected for a table before
     * its first DML appeared (so the catalog entry was not yet cached).
     *
     * @param pending a list of {@link PgAttributeChange} objects accumulated for this
     *                table's OID; may be null or empty
     * @return a new RelationInfo with changes applied, or the original if no changes
     */
    public static RelationInfo applyPendingChanges(RelationInfo rel, java.util.List<PgAttributeChange> pending) {
        if (pending == null || pending.isEmpty()) {
            return rel;
        }
        List<ColumnInfo> columns = new ArrayList<>(rel.columns);
        for (PgAttributeChange c : pending) {
            switch (c.op) {
                case "INSERT": {
                    ColumnInfo col = new ColumnInfo(c.attname, c.attnum, c.atttypid, c.attlen, c.attalign, c.attisdropped);
                    int pos = 0;
                    while (pos < columns.size() && columns.get(pos).attnum < c.attnum) {
                        pos++;
                    }
                    columns.add(pos, col);
                    break;
                }
                case "UPDATE": {
                    for (int i = 0; i < columns.size(); i++) {
                        if (columns.get(i).attnum == c.attnum) {
                            columns.set(i, new ColumnInfo(c.attname, c.attnum, c.atttypid, c.attlen, c.attalign, c.attisdropped));
                            break;
                        }
                    }
                    break;
                }
                case "DELETE":
                    columns.removeIf(col -> col.attnum == c.attnum);
                    break;
            }
        }
        return new RelationInfo(rel.schema, rel.table, columns,
                new ArrayList<>(rel.keyColumns), rel.replicaIdentityFull);
    }

    /**
     * Captures a single pg_attribute row change decoded from WAL, stored until
     * the affected table's RelationInfo is loaded into the cache.
     */
    public static final class PgAttributeChange {
        public final String op;       // "INSERT", "UPDATE", "DELETE"
        public final int attnum;
        public final String attname;
        public final long atttypid;
        public final int attlen;
        public final char attalign;
        public final boolean attisdropped;

        public PgAttributeChange(String op, int attnum, String attname, long atttypid,
                                  int attlen, char attalign, boolean attisdropped) {
            this.op = op;
            this.attnum = attnum;
            this.attname = attname;
            this.atttypid = atttypid;
            this.attlen = attlen;
            this.attalign = attalign;
            this.attisdropped = attisdropped;
        }
    }

    private RelationInfo load(long relNumber) {
        try {
            long[] oid = {0};
            String[] name = new String[2];
            boolean[] riFull = {false};
            jdbcContext.query(String.format(REL_BY_FILENODE, relNumber), rs -> {
                if (rs.next()) {
                    oid[0] = rs.getLong("oid");
                    name[0] = rs.getString("nspname");
                    name[1] = rs.getString("relname");
                    String r = rs.getString("relreplident");
                    riFull[0] = r != null && !r.isEmpty() && r.charAt(0) == 'f';
                }
            });
            if (oid[0] == 0) {
                return null;
            }
            List<ColumnInfo> columns = new ArrayList<>();
            jdbcContext.query(String.format(COLUMNS, oid[0]), rs -> {
                while (rs.next()) {
                    String align = rs.getString("attalign");
                    columns.add(new ColumnInfo(
                            rs.getString("attname"),
                            rs.getInt("attnum"),
                            rs.getLong("atttypid"),
                            rs.getInt("attlen"),
                            align == null || align.isEmpty() ? 'c' : align.charAt(0),
                            rs.getBoolean("attisdropped")));
                }
            });
            List<String> keys = new ArrayList<>();
            jdbcContext.query(String.format(KEYS, oid[0]), rs -> {
                while (rs.next()) {
                    keys.add(rs.getString("attname"));
                }
            });
            return new RelationInfo(name[0], name[1], columns, keys, riFull[0]);
        } catch (Throwable e) {
            log.warn("Failed to resolve relation for relfilenode {}: {}", relNumber, e.getMessage());
            return null;
        }
    }
}
