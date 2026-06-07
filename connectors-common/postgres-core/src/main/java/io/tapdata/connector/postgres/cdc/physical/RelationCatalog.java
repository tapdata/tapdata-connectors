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
            "SELECT c.oid, n.nspname, c.relname FROM pg_class c " +
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

    private RelationInfo load(long relNumber) {
        try {
            long[] oid = {0};
            String[] name = new String[2];
            jdbcContext.query(String.format(REL_BY_FILENODE, relNumber), rs -> {
                if (rs.next()) {
                    oid[0] = rs.getLong("oid");
                    name[0] = rs.getString("nspname");
                    name[1] = rs.getString("relname");
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
            return new RelationInfo(name[0], name[1], columns, keys);
        } catch (Throwable e) {
            log.warn("Failed to resolve relation for relfilenode {}: {}", relNumber, e.getMessage());
            return null;
        }
    }
}
