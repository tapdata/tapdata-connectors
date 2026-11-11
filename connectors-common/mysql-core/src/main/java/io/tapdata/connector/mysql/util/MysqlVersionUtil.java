package io.tapdata.connector.mysql.util;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Locale;

/** Syntax differences between MySQL 8.4+ and compatible servers with independent version numbers. */
public final class MysqlVersionUtil {
    private MysqlVersionUtil() {
    }

    public static boolean usesBinaryLogStatus(DatabaseMetaData metadata) throws SQLException {
        // Connector/J reports MySQL as the product name even when connected to MariaDB.
        String product = metadata.getDatabaseProductName();
        String version = metadata.getDatabaseProductVersion();
        if (containsMariaDb(product) || containsMariaDb(version)) {
            return false;
        }
        int major = metadata.getDatabaseMajorVersion();
        int minor = metadata.getDatabaseMinorVersion();
        return major > 8 || (major == 8 && minor >= 4);
    }

    private static boolean containsMariaDb(String value) {
        return value != null && value.toLowerCase(Locale.ROOT).contains("mariadb");
    }
}
