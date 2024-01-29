package io.tapdata.connector.gauss;

import com.huawei.opengauss.jdbc.Driver;
import com.huawei.opengauss.jdbc.PGProperty;
import com.huawei.opengauss.jdbc.util.PGobject;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class SqlMain {
  //private static final Logger logger = LogManager.getLogger(SqlMain.class);
  public static final String slotName = "slot1";
  public static final String hostname = "121.37.171.158";
  public static final int port = 8000;
  public static final String dbName = "postgres";
  public static final String schema = "public";
  public static final String username = "root";
  public static final String password = "Gotapd8!";
  public static final String driver = "com.huawei.opengauss.jdbc.Driver";
  public static final String jdbcUrl = String.format("jdbc:opengauss://%s:%d/%s", hostname, port, dbName);

  public static void main(String[] args) throws Exception {

   try (Connection conn = connection();
      Statement s = conn.createStatement();
      ResultSet rs = s.executeQuery("SELECT * FROM pg_logical_slot_peek_changes('" + slotName + "', null, 4096)")) {
    String location;
    PGobject xid;
    Object data;
    while (rs.next()) {
     location = rs.getString(1);
     xid = rs.getObject(2, PGobject.class);
     data = rs.getString(3);
     //logger.info("row: {}, {}, {}", location, xid, data);
    }
   }
  }

  static Connection connection() throws Exception {
   Class<Driver> driverClass = (Class<Driver>) Class.forName(SqlMain.driver);
   Properties properties = new Properties();
   PGProperty.USER.set(properties, username);
   PGProperty.PASSWORD.set(properties, password);
   PGProperty.LOGGER.set(properties, "Slf4JLogger");
   PGProperty.CURRENT_SCHEMA.set(properties, schema);
   PGProperty.PREFER_QUERY_MODE.set(properties, "simple");
   return driverClass.newInstance().connect(jdbcUrl, properties);
  }
}