package io.tapdata.connector.mrs;

import cn.hutool.core.codec.Base64;
import io.tapdata.connector.hive.HiveJdbcContext;
import io.tapdata.connector.mrs.config.MrsHive3Config;
import io.tapdata.connector.mrs.util.KerberosUtil;
import io.tapdata.connector.mrs.util.Krb5Util;

import java.sql.Connection;
import java.sql.SQLException;

public class MrsHive3JdbcContext extends HiveJdbcContext {

    public MrsHive3JdbcContext(MrsHive3Config mrsHive3Config) {
        super(mrsHive3Config);
    }

    public Connection getConnection() throws SQLException {
        Connection connection = super.getConnection();
        connection.setAutoCommit(true);
        return connection;
    }


}
