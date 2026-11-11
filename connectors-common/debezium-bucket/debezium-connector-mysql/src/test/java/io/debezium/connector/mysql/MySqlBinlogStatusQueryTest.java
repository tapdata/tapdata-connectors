package io.debezium.connector.mysql;

import java.lang.reflect.Proxy;
import java.sql.DatabaseMetaData;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MySqlBinlogStatusQueryTest {
    @Test
    public void mariaDbKeepsMasterStatusDespiteItsHigherVersionNumber() throws Exception {
        assertQuery("MySQL", "12.0.2-MariaDB", 12, 0, "SHOW MASTER STATUS");
        assertQuery("MariaDB", "12.0.2", 12, 0, "SHOW MASTER STATUS");
        assertQuery("MySQL", "10.11.14-MariaDB", 10, 11, "SHOW MASTER STATUS");
    }

    @Test
    public void mysqlUsesTheNewStatusSyntaxStartingAt84() throws Exception {
        assertQuery("MySQL", "8.0.42", 8, 0, "SHOW MASTER STATUS");
        assertQuery("MySQL", "8.3.0", 8, 3, "SHOW MASTER STATUS");
        assertQuery("MySQL", "8.4.0", 8, 4, "SHOW BINARY LOG STATUS");
        assertQuery("MySQL", "9.0.0", 9, 0, "SHOW BINARY LOG STATUS");
    }

    private static void assertQuery(String product, String version, int major, int minor, String sql) throws Exception {
        DatabaseMetaData metadata = (DatabaseMetaData) Proxy.newProxyInstance(
                DatabaseMetaData.class.getClassLoader(), new Class<?>[] {DatabaseMetaData.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getDatabaseProductName": return product;
                        case "getDatabaseProductVersion": return version;
                        case "getDatabaseMajorVersion": return major;
                        case "getDatabaseMinorVersion": return minor;
                        default: throw new UnsupportedOperationException(method.getName());
                    }
                });
        assertEquals(sql, MySqlConnection.binlogStatusQuery(metadata));
    }
}
