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
}
