package io.tapdata.connector.postgres.partition;

import io.tapdata.common.JdbcContext;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.partition.wrappper.PGPartitionWrapper;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.TapPartition;
import io.tapdata.entity.schema.partition.TapSubPartitionTableInfo;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.connector.common.vo.TapPartitionResult;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class PostgresPartitionContext {
    final Log tapLogger;
    String postgresVersion;
    JdbcContext jdbcContext;
    PostgresConfig postgresConfig;

    public PostgresPartitionContext(Log tapLogger) {
        this.tapLogger = tapLogger;
    }

    public PostgresPartitionContext withPostgresVersion(String postgresVersion) {
        this.postgresVersion = postgresVersion;
        return this;
    }

    public PostgresPartitionContext withJdbcContext(JdbcContext jdbcContext) {
        this.jdbcContext = jdbcContext;
        return this;
    }

    public PostgresPartitionContext withPostgresConfig(PostgresConfig postgresConfig) {
        this.postgresConfig = postgresConfig;
        return this;
    }

    public void discoverPartitionInfoByParentName(TapConnectorContext connectorContext, List<TapTable> table, Consumer<Collection<TapPartitionResult>> consumer) throws SQLException {
        List<String> tables = table.stream()
                .map(TapTable::getId)
                .collect(Collectors.toList());
        final Map<String, TapPartitionResult> tableMap = new HashMap<>();
        try {
            final String tableSQL = getTableSQL(tables);
            jdbcContext.query(String.format(SQL_MORE_10_VERSION_SELECT_SUB_TABLE, tableSQL, postgresConfig.getSchema()), r -> {
                while (r.next()) {
                    String parent = r.getString(TableType.KEY_PARENT_TABLE);
                    String sub = r.getString(TableType.KEY_PARTITION_TABLE);
                    tableMap.computeIfAbsent(parent, TapPartitionResult::create).addSubTable(sub);
                }
            });
        } finally {
            if (!tableMap.isEmpty()) {
                consumer.accept(tableMap.values());
            }
        }
    }

    protected String getTableSQL(Collection<String> table) {
        StringJoiner tables = new StringJoiner(",");
        table.forEach(name -> tables.add(String.format("'%s'", name)));
        return tables.toString();
    }

    public List<TapTable> discoverPartitionInfo(List<TapTable> tapTableList) {
        if (null == tapTableList || tapTableList.isEmpty()) return tapTableList;
        Map<String, TapTable> collect = tapTableList.stream().filter(Objects::nonNull).collect(Collectors.toMap(TapTable::getId, t -> t, (t1, t2) -> t1));
        if (collect.isEmpty()) return tapTableList;
        try {
            Set<String> removeTableIds = discoverPartitionInfo(collect);
            //移除子表
            Optional.ofNullable(removeTableIds).ifPresent(ids -> ids.forEach(id -> tapTableList.removeIf(t -> id.equals(t.getId()))));
        } catch (SQLException e) {
            tapLogger.error(e.getMessage(), e);
        }
        return tapTableList;
    }

    protected Set<String> discoverPartitionInfo(Map<String, TapTable> tapTableList) throws SQLException {
        if (null == tapTableList || tapTableList.isEmpty()) return null;
        if (Integer.parseInt(postgresVersion) < 100000) {
            // 6.3以下版本不支持分区 | 10.0版本 只支持继承方式创建表 ---> 不属于分区表，暂不支持
            tapLogger.info("Not support partition table when {} version pg", postgresVersion);
            return null;
        }

        Set<String> removeTableIds = new HashSet<>();
        Map<String, TapPartition> partitionMap = new HashMap<>();
        //10.0及以上版本 支持支持声明式分区和  继承式分区表(继承方式创建表 ---> 不属于分区表)
        final String tableSQL = getTableSQL(tapTableList.keySet());
        jdbcContext.query(String.format(SQL_MORE_10_VERSION, postgresConfig.getSchema(), tableSQL, tableSQL), r -> {
            while (r.next()) {
                String tableType = String.valueOf(r.getString(TableType.KEY_TABLE_TYPE)).trim();
                String parent = r.getString(TableType.KEY_PARENT_TABLE);
                String partitionTable = r.getString(TableType.KEY_PARTITION_TABLE);

                if (null == parent || "".equals(parent.trim())) {
                    parent = partitionTable;
                }

                String tableName = r.getString(TableType.KEY_TABLE_NAME);
                String checkOrPartitionRule = r.getString(TableType.KEY_CHECK_OR_PARTITION_RULE);
                String partitionType = String.valueOf(r.getString(TableType.KEY_PARTITION_TYPE)).trim();
                if (TableType.INHERIT.equalsIgnoreCase(partitionType)) {
                    //移除继承方式创建的子表
                    if (TableType.CHILD_TABLE.equalsIgnoreCase(tableType)) {
                        removeTableIds.add(tableName);
                    }
                    continue;
                }
                String partitionBound = String.valueOf(r.getString(TableType.KEY_PARTITION_BOUND));
                TapPartition tapPartition = partitionMap.computeIfAbsent(parent, k -> TapPartition.create());
                TapTable tapParent = tapTableList.get(parent);
                switch (tableType) {
                    case TableType.PARTITIONED_TABLE:
                        tapPartition.originPartitionStageSQL(checkOrPartitionRule);
                        tapParent.setPartitionMasterTableId(parent);
                        Optional.ofNullable(PGPartitionWrapper.partitionFields(tapParent, partitionType, checkOrPartitionRule, tableName, tapLogger))
                                .ifPresent(tapPartition::addAllPartitionFields);
                    case TableType.PARENT_TABLE:
                        Optional.ofNullable(PGPartitionWrapper.type(partitionType, tableName, tapLogger))
                                .ifPresent(tapPartition::type);
                        break;
                    case TableType.CHILD_TABLE:
                        TapTable subTable = tapTableList.get(tableName);
                        TapSubPartitionTableInfo partitionSchema = TapSubPartitionTableInfo.create()
                                .originPartitionBoundSQL(partitionBound)
                                .tableName(tableName);
                        Optional.ofNullable(PGPartitionWrapper.warp(tapParent, partitionType, checkOrPartitionRule, partitionBound, tapLogger))
                                .ifPresent(partitionSchema::setTapPartitionTypes);
                        tapPartition.appendPartitionSchemas(partitionSchema);

                        if (null == subTable) continue;
                        subTable.partitionMasterTableId(parent);
                        subTable.setPartitionInfo(tapPartition); //子表要包含分区信息
                        break;
                    default:
                }
            }
        });
        partitionMap.forEach((key, partition) -> Optional.ofNullable(tapTableList.get(key)).ifPresent(tab -> tab.setPartitionInfo(partition)));
        return removeTableIds;
    }

    public static final String SQL_MORE_10_VERSION_SELECT_SUB_TABLE = "SELECT " +
            "    parent.relname AS " + TableType.KEY_PARENT_TABLE + ", " +
            "    child.relname AS " + TableType.KEY_PARTITION_TABLE + " " +
            "FROM " +
            "    pg_inherits AS i " +
            "        JOIN pg_class AS child ON i.inhrelid = child.oid " +
            "        JOIN pg_class AS parent ON i.inhparent = parent.oid " +
            "        JOIN pg_namespace AS nsp ON child.relnamespace = nsp.oid " +
            "        JOIN pg_partitioned_table AS pt ON parent.oid = pt.partrelid " +
            "WHERE " +
            "   parent.relname IN (%s) " +
            "   AND nsp.nspname = '%s' " +
            "ORDER BY " +
            "    parent.relname, child.relname ";

    public static final String SQL_MORE_10_VERSION = "WITH  " +
            "all_tables AS (  " +
            "    SELECT  " +
            "        c.oid AS table_oid,  " +
            "        c.relfilenode as relfilenode,  " +
            "        c.relname AS table_name,  " +
            "        n.nspname AS schema_name,  " +
            "        i.inhparent    as inhparent,  " +
            "        CASE  " +
            "            WHEN p.partrelid IS NOT NULL THEN '" + TableType.PARTITIONED_TABLE + "'  " +
            "            WHEN i.inhrelid IS NOT NULL THEN '" + TableType.CHILD_TABLE + "'  " +
            "            WHEN c.relhassubclass = true THEN '" + TableType.PARENT_TABLE + "'  " +
            "            ELSE '" + TableType.REGULAR_TABLE + "'  " +
            "            END AS table_type  " +
            "    FROM  " +
            "        pg_class c  " +
            "            JOIN pg_namespace n ON c.relnamespace = n.oid  " +
            "            LEFT JOIN pg_partitioned_table p ON c.oid = p.partrelid  " +
            "            LEFT JOIN pg_inherits i ON c.oid = i.inhrelid  " +
            "    WHERE  " +
            "            n.nspname = '%s' and (c.relname in (%s) OR inhparent in (select oid from pg_class where relname in (%s))) " +
            "),  " +
            "  " +
            "inherits AS (  " +
            "    SELECT  " +
            "        parent.relname AS parent_table,  " +
            "        child.relname AS child_table,  " +
            "        pg_inherits.inhrelid as inhrel_id  " +
            "    FROM  " +
            "        pg_inherits  " +
            "            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid  " +
            "            JOIN pg_class child ON pg_inherits.inhrelid = child.oid  " +
            "),  " +
            "  " +
            "inherits_check AS (  " +
            "    SELECT  " +
            "          parent.relname AS parent_table,  " +
            "          child.relname AS partition_table,  " +
            "          conname AS constraint_name,  " +
            "          pg_catalog.pg_get_constraintdef(con.oid) AS constraint_definition  " +
            "      FROM  " +
            "          pg_class parent  " +
            "              JOIN  " +
            "          pg_inherits i ON parent.oid = i.inhparent  " +
            "              JOIN  " +
            "          pg_class child ON i.inhrelid = child.oid  " +
            "              JOIN  " +
            "          pg_constraint con ON child.oid = con.conrelid  " +
            "),  " +
            "  " +
            "partition_columns AS (  " +
            "    SELECT  " +
            "        a.attrelid AS partitioned_table_oid,  " +
            "        a.attname AS partition_column  " +
            "    FROM  " +
            "        pg_attribute a  " +
            "            JOIN pg_partitioned_table pt ON a.attrelid = pt.partrelid  " +
            "    WHERE a.attnum = ANY(pt.partattrs)  " +
            "),  " +
            "  " +
            "partitions AS (  " +
            "    SELECT  " +
            "        pt.partrelid AS partitioned_table_oid,  " +
            "        c.relname AS partition_table,  " +
            "        pt.partstrat AS partition_strategy  " +
            "    FROM  " +
            "        pg_partitioned_table pt  " +
            "            JOIN pg_class c ON pt.partrelid = c.oid  " +
            "),  " +
            "  " +
            "partition_bounds AS (  " +
            "    SELECT  " +
            "        pt.partstrat AS partition_strategy,  " +
            "        inhrelid AS table_oid,  " +
            "        c.relname AS child_table,  " +
            "        pg_get_expr(c.relpartbound, c.oid) AS partition_bound  " +
            "    FROM  " +
            "        pg_inherits i  " +
            "            JOIN pg_class c ON i.inhrelid = c.oid  " +
            "            LEFT JOIN pg_partitioned_table pt ON i.inhparent = pt.partrelid  " +
            ")  " +
            "  " +
            "SELECT  " +
            "    a.table_name AS " + TableType.KEY_TABLE_NAME + ",  " +
            "    a.table_type AS " + TableType.KEY_TABLE_TYPE + " ,  " +
            "    CASE  " +
            "        WHEN a.table_type = '" + TableType.PARENT_TABLE + "' THEN '" + TableType.INHERIT + "'  " +
            "        ELSE COALESCE(  " +
            "                CASE  " +
            "                    WHEN p.partition_strategy = 'r' OR pb.partition_strategy = 'r' THEN '" + TableType.RANGE + "'  " +
            "                    WHEN p.partition_strategy = 'l' OR pb.partition_strategy = 'l' THEN '" + TableType.LIST + "'  " +
            "                    WHEN p.partition_strategy = 'h' OR pb.partition_strategy = 'h' THEN '" + TableType.HASH + "'  " +
            "                    WHEN a.table_type = '" + TableType.CHILD_TABLE + "' THEN '" + TableType.INHERIT + "'  " +
            "                    ELSE '" + TableType.UN_KNOW + "'  " +
            "                    END,  " +
            "                '')  " +
            "        END AS " + TableType.KEY_PARTITION_TYPE + ",  " +
            "    CASE  " +
            "        WHEN a.table_type = 'Partitioned Table' THEN pg_get_partkeydef(concat(a.schema_name, '.', a.table_name)::REGCLASS)  " +
            "        WHEN i.parent_table IS NOT NULL THEN  " +
            "            COALESCE(pg_get_partkeydef(concat(a.schema_name, '.', i.parent_table)::REGCLASS), COALESCE(ic.constraint_definition, ''))  " +
            "        ELSE '' END AS " + TableType.KEY_CHECK_OR_PARTITION_RULE + ",  " +
            "    COALESCE(pb.partition_bound, '') AS " + TableType.KEY_PARTITION_BOUND + ",  " +
            "    COALESCE(i.parent_table, '') AS " + TableType.KEY_PARENT_TABLE + ",  " +
            "    COALESCE(p.partition_table, '') AS " + TableType.KEY_PARTITION_TABLE + "  " +
            "FROM  " +
            "    all_tables a  " +
            "        LEFT JOIN inherits i ON a.table_name = i.child_table  " +
            "        LEFT JOIN partitions p ON a.table_oid = p.partitioned_table_oid  " +
            "        LEFT JOIN inherits_check ic ON a.table_name = ic.partition_table  " +
            "        LEFT JOIN partition_bounds pb ON a.table_oid = pb.table_oid  " +
            "WHERE a.table_type <> '" + TableType.REGULAR_TABLE + "'  " +
            "ORDER BY  " +
            "    a.schema_name,  " +
            "    a.table_type DESC ";
}
