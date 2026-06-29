package io.tapdata.connector.postgres.error;

import io.tapdata.exception.TapExClass;
import io.tapdata.exception.TapExCode;
import io.tapdata.exception.TapExLevel;
import io.tapdata.exception.TapExType;

@TapExClass(
        code = 41,
        module = "postgres-connector",
        describe = "Postgres Error Code",
        prefix = "PG")
public interface PostgresErrorCode {
    @TapExCode(
            describe = "Failed to create publication, please analyze the specific error message. \n" +
                    "Common errors and reasons: \n" +
                    " ERROR: permission denied for database. \n" +
                    "The current user does not have permission to create publications.",
            describeCN = "创建 publication 失败，需结合具体报错信息分析。\n" +
                    "常见的报错及原因：\n" +
                    "ERROR: permission denied for database. \n" +
                    "当前用户没有创建 publication 的权限。",
            solution = "Solution (choose one): \n" +
                    "1. Use a superuser connection (such as postgres). \n" +
                    "2. Grant the current user the required permissions and execute: ALTER USER username REPLICATION;",
            solutionCN = "解决方案（任选其一）：\n" +
                    "1. 使用超级用户连接（例如 postgres）。\n" +
                    "2. 赋予当前用户所需权限，执行：ALTER USER username REPLICATION;",
            dynamicDescription = "Execute sql failed: {}",
            dynamicDescriptionCN = "执行语句失败：{}",
            level = TapExLevel.CRITICAL,
            type = TapExType.RUNTIME
    )
    String CREATE_PUBLICATION_FAILED = "410001";

    @TapExCode(
            describe = "Failed to select publication, please analyze the specific error message. \n" +
                    "Common errors and reasons: \n" +
                    "ERROR: must be superuser or replication role to use replication slots. \n" +
                    "The current user does not have permission to create publications.",
            describeCN = "查询 publication 失败，需结合具体报错信息分析。\n" +
                    "常见的报错及原因：\n" +
                    "ERROR: must be superuser or replication role to use replication slots \n" +
                    "当前用户没有查询 publication 的权限。",
            solution = "Solution (choose one): \n" +
                    "1. Use a superuser connection (such as postgres). \n" +
                    "2. Grant the current user the required permissions and execute: ALTER USER username REPLICATION;",
            solutionCN = "解决方案（任选其一）：\n" +
                    "1. 使用超级用户连接（例如 postgres）。\n" +
                    "2. 赋予当前用户所需权限，执行：ALTER USER username REPLICATION;",
            dynamicDescription = "Execute sql failed: {}",
            dynamicDescriptionCN = "执行语句失败：{}",
            level = TapExLevel.CRITICAL,
            type = TapExType.RUNTIME
    )
    String SELECT_PUBLICATION_FAILED = "410002";

    @TapExCode(
            describe = "Failed to create slot, please analyze the specific error message. \n" +
                    "Common errors and reasons: \n" +
                    "ERROR: must be superuser or replication role to use replication slots. \n" +
                    "The current user does not have permission to create slots.",
            describeCN = "创建 slot 失败，需结合具体报错信息分析。\n" +
                    "常见的报错及原因：\n" +
                    "ERROR: must be superuser or replication role to use replication slots \n" +
                    "当前用户没有创建 slot 的权限。",
            solution = "Solution (choose one): \n" +
                    "1. Use a superuser connection (such as postgres). \n" +
                    "2. Grant the current user the required permissions and execute: ALTER USER username REPLICATION;",
            solutionCN = "解决方案（任选其一）：\n" +
                    "1. 使用超级用户连接（例如 postgres）。\n" +
                    "2. 赋予当前用户所需权限，执行：ALTER USER username REPLICATION;",
            dynamicDescription = "Execute sql failed: {}",
            dynamicDescriptionCN = "执行语句失败：{}",
            level = TapExLevel.CRITICAL,
            type = TapExType.RUNTIME
    )
    String CREATE_SLOT_FAILED = "410003";

    @TapExCode(
            describe = "Failed to create slot, please analyze the specific error message. \n" +
                    "Common errors and reasons: \n" +
                    "ERROR: timeout expired. \n" +
                    "The creation of slot timed out, which is probably affected by a long time of uncommitted transaction or other database processes blocking wait.",
            describeCN = "创建 slot 超时，大概率是受长时间未提交事务影响或其它数据库进程阻塞等待",
            solution = "Solution (cautious handling): \n" +
                    "1. Please check if there are any long uncommitted transactions in the database. If so, please commit them first. \n" +
                    "2. Please check if there are any other database processes blocking wait. If so, please handle them first.\n" +
                    "SELECT pid, datname, usename, state, xact_start, now() - xact_start AS duration, query\n" +
                    "FROM pg_stat_activity\n" +
                    "WHERE state != 'idle'\n" +
                    "  AND xact_start IS NOT NULL\n" +
                    "  AND now() - xact_start > interval '300 seconds'  -- for example, find transactions that have been running for more than 5 minutes\n" +
                    "ORDER BY xact_start;",
            solutionCN = "解决方案（需谨慎处理）：\n" +
                    "1. 请检查数据库是否存在长时间未提交事务，如存在请先提交事务。\n" +
                    "SELECT pid, datname, usename, state, xact_start, now() - xact_start AS duration, query\n" +
                    "FROM pg_stat_activity\n" +
                    "WHERE state != 'idle'\n" +
                    "  AND xact_start IS NOT NULL\n" +
                    "  AND now() - xact_start > interval '300 seconds'  -- 例如，查找超过5分钟的事务\n" +
                    "ORDER BY xact_start;\n" +
                    "2. 请检查数据库是否存在其它数据库进程阻塞等待，如存在请先处理阻塞。",
            dynamicDescription = "Execute sql failed: {}",
            dynamicDescriptionCN = "执行语句失败：{}",
            level = TapExLevel.CRITICAL,
            type = TapExType.RUNTIME,
            recoverable = true
    )
    String CREATE_SLOT_TIMEOUT = "410004";

    @TapExCode(
            describe = "Failed to create DDL event trigger artifacts (audit table / trigger function / event trigger). \n" +
                    "Common errors and reasons: \n" +
                    "1. The current user does not have superuser privileges to create event triggers. \n" +
                    "2. The specified schema does not exist or the user lacks CREATE permission on it. \n" +
                    "3. An artifact with the same name already exists from a previous run.",
            describeCN = "创建 DDL 事件触发器物件（审计表 / 触发器函数 / 事件触发器）失败。\n" +
                    "常见的报错及原因：\n" +
                    "1. 当前用户没有超级用户权限来创建事件触发器。\n" +
                    "2. 指定的 schema 不存在或用户缺少 CREATE 权限。\n" +
                    "3. 上一次运行的 artifact 残留导致同名冲突。",
            solution = "Solution: \n" +
                    "1. Use a superuser connection (such as postgres) to run the task, or \n" +
                    "2. Grant the current user superuser privileges, or \n" +
                    "3. Clean up existing artifacts manually: \n" +
                    "   DROP EVENT TRIGGER IF EXISTS _tapdata_intercept_ddl; \n" +
                    "   DROP FUNCTION IF EXISTS <schema>._tapdata_intercept_ddl(); \n" +
                    "   DROP TABLE IF EXISTS <schema>._tapdata_ddl_audit; \n" +
                    "4. Ensure the DDL trigger schema exists and is accessible.",
            solutionCN = "解决方案：\n" +
                    "1. 使用超级用户连接（例如 postgres）运行任务，或\n" +
                    "2. 赋予当前用户超级用户权限，或\n" +
                    "3. 手动清理已存在的 artifact：\n" +
                    "   DROP EVENT TRIGGER IF EXISTS _tapdata_intercept_ddl; \n" +
                    "   DROP FUNCTION IF EXISTS <schema>._tapdata_intercept_ddl(); \n" +
                    "   DROP TABLE IF EXISTS <schema>._tapdata_ddl_audit; \n" +
                    "4. 确保 DDL trigger schema 存在且可访问。",
            dynamicDescription = "Setup DDL trigger artifacts failed: {}",
            dynamicDescriptionCN = "设置 DDL 触发器物件失败：{}",
            level = TapExLevel.CRITICAL,
            type = TapExType.RUNTIME
    )
    String CREATE_DDL_TRIGGER_FAILED = "410005";

    @TapExCode(
            describe = "Failed to clean up DDL event trigger artifacts. \n" +
                    "This is usually not critical — the artifacts may have already been removed or the connection was lost. \n" +
                    "Check the database manually if artifacts persist.",
            describeCN = "清理 DDL 事件触发器物件失败。\n" +
                    "通常不是严重问题 — artifact 可能已被移除或连接已断开。\n" +
                    "如果 artifact 残留，请手动检查数据库。",
            solution = "Manually clean up: \n" +
                    "DROP EVENT TRIGGER IF EXISTS _tapdata_intercept_ddl; \n" +
                    "DROP FUNCTION IF EXISTS <schema>._tapdata_intercept_ddl(); \n" +
                    "DROP TABLE IF EXISTS <schema>._tapdata_ddl_audit;",
            solutionCN = "手动清理：\n" +
                    "DROP EVENT TRIGGER IF EXISTS _tapdata_intercept_ddl; \n" +
                    "DROP FUNCTION IF EXISTS <schema>._tapdata_intercept_ddl(); \n" +
                    "DROP TABLE IF EXISTS <schema>._tapdata_ddl_audit;",
            dynamicDescription = "Cleanup DDL trigger artifacts failed: {}",
            dynamicDescriptionCN = "清理 DDL 触发器物件失败：{}",
            level = TapExLevel.NORMAL,
            type = TapExType.RUNTIME
    )
    String DROP_DDL_TRIGGER_FAILED = "410006";

    @TapExCode(
            describe = "DDL trigger is enabled but the log plugin does not support it. \n" +
                    "DDL trigger only works with logical replication plugins: pgoutput, wal2json, decoderbufs. \n" +
                    "It does NOT work with walminer or physical replication.",
            describeCN = "DDL 触发器已启用但当前日志插件不支持。\n" +
                    "DDL 触发器仅支持逻辑复制插件：pgoutput, wal2json, decoderbufs。\n" +
                    "不支持 walminer 或 physical 复制。",
            solution = "Solution: \n" +
                    "1. Switch the log plugin to pgoutput, wal2json, or decoderbufs, or \n" +
                    "2. Disable DDL trigger by setting ddlTriggerEnable=false.",
            solutionCN = "解决方案：\n" +
                    "1. 将日志插件切换为 pgoutput、wal2json 或 decoderbufs，或\n" +
                    "2. 将 ddlTriggerEnable 设置为 false 以禁用 DDL 触发器。",
            dynamicDescription = "DDL trigger unsupported for plugin: {}",
            dynamicDescriptionCN = "DDL 触发器不支持当前插件：{}",
            level = TapExLevel.CRITICAL,
            type = TapExType.RUNTIME
    )
    String DDL_TRIGGER_UNSUPPORTED_PLUGIN = "410007";
}
