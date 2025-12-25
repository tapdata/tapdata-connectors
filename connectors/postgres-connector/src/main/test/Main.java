import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.entity.utils.DataMap;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class Main {

    public void main() throws Throwable {
        // Step 1: Configure log4jdbc (replaces log4jdbc.log4j2.properties file)
        // MUST be called BEFORE any JDBC driver loading
//        Log4jdbcConfigHelper.configureDefault();  // or use configureDebug(), configureMinimal(), etc.
//
//        // Step 2: Configure Log4j2 (replaces log4j2.properties file)
//        String taskLoggerName = "task.postgres.jdbc.001";
//        Log4j2ConfigHelper.configureForTask(
//                "jarad",
//                true,   // enableSqlLogging - set to true to see SQL statements
//                true,   // enableTimingLogging - set to true to see SQL execution time
//                false   // enableConnectionLogging - set to false to hide connection logs
//        );
//
//        // Step 3: Set custom logger name for current thread
//        CustomLogDelegator.setLoggerName(taskLoggerName);
        System.setProperty("log4jdbc.spylogdelegator.name", "io.tapdata.common.log.CustomLogDelegator");
        DataMap map = new DataMap();
        map.put("host", "127.0.0.1");
        map.put("port", 5432);
        map.put("database", "postgres");
        map.put("schema", "public");
        map.put("extParams", "");
        map.put("user", "postgres");
        map.put("password", "gj0628");
        PostgresConfig postgresConfig = (PostgresConfig) new PostgresConfig().load(map);
//        postgresConfig.setHost("192.168.1.189");
//        postgresConfig.setPort(5432);
//        postgresConfig.setDatabase("COOLGJ");
//        postgresConfig.setSchema("public");
//        postgresConfig.setExtParams("");
//        postgresConfig.setUser("postgres");
//        postgresConfig.setPassword("gj0628");
        try (
                PostgresJdbcContext postgresJdbcContext = new PostgresJdbcContext(postgresConfig);
                Connection connection = postgresJdbcContext.getConnection();
                PreparedStatement preparedStatement = connection.prepareStatement("insert into public.testexp (a1,a2) values(?,?)")
        ) {
//            ResultSet rs = statement.executeQuery("select * from public.\"CAR_CLAIM\"");
//            if(rs.next()) {
//                System.out.println(rs.getString("CLAIM_ID"));
//            }
            preparedStatement.setObject(1, 2);
            preparedStatement.setObject(2, "dsf");
            preparedStatement.addBatch();
            preparedStatement.setObject(1, 3);
            preparedStatement.setObject(2, "dsf");
            preparedStatement.addBatch();
            preparedStatement.executeBatch();
            connection.commit();
//            statement.executeUpdate("insert into public.\"CAR_CLAIM\" (\"CLAIM_ID\") values(11)");
//            String updateSql = "WITH upsert AS (UPDATE \"public\".\"aabbccdd\" SET \"aa\"=?,\"bb\"=?,\"game\"=?,\"FUCK\"=? WHERE \"aa\"=? RETURNING *)" +
//                    " INSERT INTO \"public\".\"aabbccdd\" (\"aa\",\"bb\",\"game\",\"FUCK\") SELECT ?,?,?,? WHERE NOT EXISTS (SELECT * FROM upsert)";
//            ExecutorService executorService = Executors.newFixedThreadPool(16);
//            CountDownLatch countDownLatch = new CountDownLatch(16);
//            for (int j = 0; j < 16; j++) {
//                executorService.submit(() -> {
//                    PreparedStatement ps = null;
//                    try {
//                        ps = connection.prepareStatement(updateSql);
//                        for (int i = 0; i < 1000; i++) {
//                            ps.setObject(1, 90);
//                            ps.setObject(2, "GJJ");
//                            ps.setObject(3, "GJSJFADSFSDFADSFASDFSADFASDFSFASFASDFASDFASDFADSFADFASDFASDF");
//                            ps.setObject(4, 718371);
//                            ps.setObject(5, 90);
//                            ps.setObject(6, 90);
//                            ps.setObject(7, "GJJ");
//                            ps.setObject(8, "GJSJFADSFSDFADSFASDFSADFASDFSFASFASDFASDFASDFADSFADFASDFASDF");
//                            ps.setObject(9, 718371);
//                            ps.addBatch();
//                            ps.clearParameters();
//                        }
//                        ps.executeBatch();
//                        ps.close();
//                        connection.commit();
//                        countDownLatch.countDown();
//                    } catch (SQLException e) {
//                        throw new RuntimeException(e);
//                    }
//                });
//            }
//            countDownLatch.await();
//            executorService.shutdown();
//        postgresJdbcContext.query("select * from \"Student\"", rs -> {
//            rs.last();
//            System.out.println(rs.getRow());
//        });
        } finally {
            // Clean up ThreadLocal to prevent memory leaks
//            CustomLogDelegator.clearLoggerName();
        }
    }
}