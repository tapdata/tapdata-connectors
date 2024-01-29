package io.tapdata.connector.gauss;

import com.huawei.opengauss.jdbc.PGProperty;
import com.huawei.opengauss.jdbc.jdbc.PgConnection;
import com.huawei.opengauss.jdbc.replication.LogSequenceNumber;
import com.huawei.opengauss.jdbc.replication.PGReplicationStream;
import com.huawei.opengauss.jdbc.replication.fluent.ReplicationStreamBuilder;
//importort org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class GaussdbReplication {
  //private static final Logger logger = LogManager.getLogger(GaussdbReplication.class);
  private static final String START_LSN = "6F/E3C53568";
  private static final String SLOT_NAME = "slot1";
  private static final String HOSTNAME = "1.94.122.172";
  private static final int PORT = 8001;
  private static final String DB_NAME = "postgres";
  private static final String SCHEMA = "public";
  private static final String USERNAME = "root";
  private static final String PASSWORD = "Gotapd8!";
  private static final String DRIVER = "com.huawei.opengauss.jdbc.Driver";
  private static final String JDBC_URL = String.format("jdbc:opengauss://%s:%d/%s", HOSTNAME, PORT, DB_NAME);
  private static final String WHITE_TABLE_LIST = String.join(",", "root.mysql_table");

  public static void main(String[] args) throws Exception {
   Class.forName(DRIVER);

   Properties properties = new Properties();
   PGProperty.USER.set(properties, USERNAME);
   PGProperty.PASSWORD.set(properties, PASSWORD);
   // 对于逻辑复制，以下三个属性是必须配置项
   PGProperty.REPLICATION.set(properties, "database");
   PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "9.4");
   PGProperty.PREFER_QUERY_MODE.set(properties, "simple");
   PGProperty.CURRENT_SCHEMA.set(properties, SCHEMA);

   // 自定义
   PGProperty.LOGGER.set(properties, "Slf4JLogger");
   PGProperty.SSL.set(properties, true);
   PGProperty.SSL_FACTORY.set(properties, "com.huawei.opengauss.jdbc.ssl.NonValidatingFactory");

   try (PgConnection conn = (PgConnection) DriverManager.getConnection(JDBC_URL, properties)) {
    //logger.info("connection success!");

    PGReplicationStream stream = conn
     .getReplicationAPI()
     .replicationStream()
     .logical()
     .withSlotName(SLOT_NAME)
     .withSlotOption("include-xids", false)
     .withSlotOption("skip-empty-xacts", true)
//      .withStartPosition(waitLSN)
     .withSlotOption("parallel-decode-num", 10) //解码线程并行度
     .withSlotOption("white-table-list", WHITE_TABLE_LIST) //白名单列表
     .withSlotOption("standby-connection", true) //强制备机解码
     .withSlotOption("decode-style", "t") //解码格式
     .withSlotOption("sending-batch", 1) //批量发送解码结果
     .withSlotOption("max-txn-in-memory", 100) //单个解码事务落盘内存阈值为100MB
     .withSlotOption("max-reorderbuffer-in-memory", 50) //正在处理的解码事务落盘内存阈值为
     .withSlotOption("exclude-users", "userA") //不返回用户userA执行事务的逻辑日志
     .withSlotOption("include-user", true) //事务BEGIN逻辑日志携带用户名字
     .withSlotOption("enable-heartbeat", true) // 开启心跳日志
     .start();

    try {
     while (!Thread.interrupted()) {
      ByteBuffer byteBuffer = stream.readPending();

      if (byteBuffer == null) {
       TimeUnit.MILLISECONDS.sleep(5L);
       continue;
      }

      int offset = byteBuffer.arrayOffset();
      byte[] source = byteBuffer.array();
      int length = source.length - offset;
         String s = new String(source, offset, length);
         if (!s.contains("HeartBeat: ")) {
             System.out.println("-------> " + s);
         }

      //如果需要flush lsn，根据业务实际情况调用以下接口
      //LogSequenceNumber lastRecv = stream.getLastReceiveLSN();
      //stream.setFlushedLSN(lastRecv);
      //stream.forceUpdateStatus();
     }
    } finally {
     stream.close();
    }
   }
  }
}