package io.tapdata.connector.mysql;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.*;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * MySQL Binary Log Client Demo
 * 演示如何使用 MySQL Binary Log Client 读取 binlog 事件
 */
public class MysqlBinaryReaderTest {

    // 测试配置 - 根据实际环境修改
    private static final String MYSQL_HOST = "localhost";
    private static final int MYSQL_PORT = 3306;
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "gj0628";
    private static final String MYSQL_SCHEMA = "COOLGJ";

    /**
     * 基本的 Binary Log Client 使用示例
     */

    void testBasicBinaryLogClient() throws Exception {
        System.out.println("=== Basic Binary Log Client Demo ===");

        // 创建 Binary Log Client
        BinaryLogClient client = new BinaryLogClient(MYSQL_HOST, MYSQL_PORT, MYSQL_USERNAME, MYSQL_PASSWORD);

        // 设置服务器 ID（必须唯一）
        client.setServerId(12345);

        // 注册事件监听器
        client.registerEventListener(new BinaryLogClient.EventListener() {
            @Override
            public void onEvent(Event event) {
                EventHeader header = event.getHeader();
                EventData data = event.getData();
            }
        });

        // 连接并开始监听
        System.out.println("连接到 MySQL: " + MYSQL_HOST + ":" + MYSQL_PORT);
        client.connect();

        // 运行 30 秒后断开
        Thread.sleep(30000);
        client.disconnect();

        System.out.println("Binary Log Client 断开连接");
    }


}
