package io.tapdata.connector.hive3;

import io.tapdata.connector.hive.HiveJdbcContext;
import io.tapdata.connector.hive.write.HdfsWrite;
import io.tapdata.entity.logger.TapLogger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class Hive3Write extends HdfsWrite {

   private static final String TAG = Hive3Write.class.getSimpleName();

   private static ReentrantLock lock = new ReentrantLock();
   private final Map<String, Connection> connectionCacheMap = new LRUOnRemoveMap<>(10, entry -> closeQuietly(entry.getValue()));


   private  final  static  String  FILE_NAME = "Hive3.txt";

   private String LOAD_SQL = "load data local inpath %s overwrite into table %s";


   public Hive3Write(HiveJdbcContext hiveJdbcContext, Hive3Config hiveConfig) {
     super(hiveJdbcContext,hiveConfig);

   }


   public void onDestroy()  {
      this.running.set(false);
      try {
         if (fs != null) {
            fs.close();
         }
      }catch (Exception e){
         TapLogger.warn(TAG, "fs  close fail:{}", e.getMessage());
      }
      this.jdbcCacheMap.values().forEach(JdbcCache::clear);
      for (Connection connection : this.connectionCacheMap.values()) {
         try {
            connection.close();
         } catch (SQLException e) {
            TapLogger.error(TAG, "connection:{} close fail:{}", connection, e.getMessage());
            throw new RuntimeException(e);
         }
      }
   }

   public Connection getConnection() {
      String name = Thread.currentThread().getName();
      Connection connection2 = connectionCacheMap.get(name);
      if (connection2 == null) {
         try {
            lock.lock();
            connection2 = connectionCacheMap.get(name);
            if (connection2 == null) {
               connection2 = hiveJdbcContext.getConnection();
               connectionCacheMap.put(name, connection2);
            }
         } catch (SQLException e) {
            throw new RuntimeException(e);
         } finally {
            lock.unlock();
         }
      }
      return connection2;
   }

}
