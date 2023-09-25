import com.mysql.cj.jdbc.ConnectionImpl;
import io.tapdata.connector.adb.AliyunMysqlConfig;
import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import io.tapdata.connector.mysql.config.MysqlConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Main {
    public static void main(String[] args) throws Exception {
//        Class.forName("com.mysql.cj.jdbc.Driver");
//        MysqlConfig mysqlConfig = new AliyunMysqlConfig();
//        mysqlConfig.setHost("8.134.184.252");
//        mysqlConfig.setPort(3306);
//        mysqlConfig.setDatabase("jarad");
//        mysqlConfig.setUser("tapdata");
//        mysqlConfig.setPassword("Gotapd8!");
//        MysqlJdbcContextV2 mysqlJdbcContextV2 = new MysqlJdbcContextV2(mysqlConfig);
        Connection connection = DriverManager.getConnection("jdbc:mysql://8.134.184.252:3306/jarad?useUnicode=yes&rewriteBatchedStatements=true&allowPublicKeyRetrieval=true&tinyInt1isBit=false&zeroDateTimeBehavior=convertToNull&characterEncoding=UTF-8&useTimezone=false&autoReconnect=true&useSSL=false", "tapdata", "Gotapd8!");
        connection.setAutoCommit(false);
//        Connection connection = mysqlJdbcContextV2.getConnection();
        PreparedStatement p = connection.prepareStatement("update ppp set col4=? where id=?");
        p.setObject(1, "test");
        p.setObject(2, "013a4b76-1700-41c3-bb76-9985b67ef14c");
        p.addBatch();
        p.clearParameters();
        for(int i=0 ;i < 2; i++) {
            p.setObject(1, "test");
            p.setObject(2, "0735a77e-dc42-44d0-b251-aeaeee01f048");
            p.addBatch();
            p.clearParameters();
        }
        p.executeBatch();
        connection.commit();
        p.close();
        connection.close();
//        mysqlJdbcContextV2.close();
    }
}
