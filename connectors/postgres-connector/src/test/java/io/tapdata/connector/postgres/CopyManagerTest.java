package io.tapdata.connector.postgres;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.tapdata.common.fileinput.RecordInputStream;
import io.tapdata.connector.postgres.config.PostgresConfig;
import org.junit.jupiter.api.Test;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.sql.Connection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CopyManagerTest {

    void testCopyManager() throws Exception {
        long begin = System.currentTimeMillis();
        PostgresConfig postgresConfig = new PostgresConfig();
        postgresConfig.setHost("localhost");
        postgresConfig.setPort(5432);
        postgresConfig.setDatabase("postgres");
        postgresConfig.setUser("postgres");
        postgresConfig.setPassword("gj0628");
        postgresConfig.setSchema("public");
        PooledByteBufAllocator allocator = new PooledByteBufAllocator();
        ByteBuf[] bufs = new ByteBuf[4];
        for (int i = 0; i < 4; i++) {
            bufs[i] = allocator.directBuffer(5000000);
        }
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        CountDownLatch countDownLatch = new CountDownLatch(4);
        PostgresJdbcContext postgresJdbcContext = new PostgresJdbcContext(postgresConfig);
        try {
            for(int ii = 0; ii < 4; ii++) {
                int finalIi = ii;
                executorService.submit(() -> {
                    try (Connection connection = postgresJdbcContext.getConnection())
                    {
                        CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
                        for (int j = 0; j < 25; j++) {
                            for (int i = 0; i < 20000; i++) {
                                bufs[finalIi].writeBytes((finalIi + "d6db2bf-e75d-4131-86b6-9dd39a632fa3" + i + "_" + j + ",D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT,D5jEQ8cT\n").getBytes());
                            }
                            try (ByteBufInputStream byteBufInputStream = new ByteBufInputStream(bufs[finalIi])) {
                                copyManager.copyIn("COPY \"tttt\" FROM STDIN DELIMITER ','", byteBufInputStream);
                            }
                            connection.commit();
                            bufs[finalIi].clear();
                        }
                    } catch (Exception e){
                        System.out.println(e);
                    } finally {
                        countDownLatch.countDown();
                        bufs[finalIi].release();
                    }
                });
            }
        } finally {
            executorService.shutdown();
            postgresJdbcContext.close();
        }
        while (!countDownLatch.await(1000, java.util.concurrent.TimeUnit.MILLISECONDS)) {
            System.out.println("waiting...");
        }
        System.out.println(System.currentTimeMillis() - begin);
    }

    void testCopyManager2() throws Exception {
        long begin = System.currentTimeMillis();
        PostgresConfig postgresConfig = new PostgresConfig();
        postgresConfig.setHost("localhost");
        postgresConfig.setPort(5432);
        postgresConfig.setDatabase("postgres");
        postgresConfig.setUser("postgres");
        postgresConfig.setPassword("gj0628");
        postgresConfig.setSchema("public");
        try (
                PostgresJdbcContext postgresJdbcContext = new PostgresJdbcContext(postgresConfig);
                Connection connection = postgresJdbcContext.getConnection();
        ) {
            CopyManager copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
            for (int j = 0; j < 100; j++) {
                try (RecordInputStream recordInputStream = new RecordInputStream(5000000)) {
                    for (int i = 0; i < 100000; i++) {
                        recordInputStream.write("1003,4\\,5sdkfjsd,ksjfldskjfskdjf\n".getBytes());
                    }
                    recordInputStream.flip();
                    copyManager.copyIn("COPY \"tttt\" FROM STDIN DELIMITER ','", recordInputStream);
                }
                connection.commit();
            }
        }
        System.out.println(System.currentTimeMillis() - begin);
    }
}
