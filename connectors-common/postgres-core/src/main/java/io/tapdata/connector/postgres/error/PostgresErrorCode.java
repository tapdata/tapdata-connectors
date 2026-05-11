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
}
