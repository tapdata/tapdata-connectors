package io.tapdata.connector.postgres;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * 测试：Timestamp → Instant → UTC时区(ZonedDateTime)
 * 验证：东八区时间转0时区是否正确（-8小时）
 */
public class TimeZoneConvertTest {

    public static void main(String[] args) {
        // ==============================================
        // 测试用例1：模拟【东八区 当前时间】
        // ==============================================
        Timestamp sourceTimestamp = new Timestamp(System.currentTimeMillis());
        System.out.println("===== 测试用例1：当前时间（东八区）转 UTC =====");
        convertAndPrint(sourceTimestamp);

        // ==============================================
        // 测试用例2：固定东八区时间 2025-01-01 12:00:00
        // 预期转 UTC 后：2025-01-01 04:00:00
        // ==============================================
        System.out.println("\n===== 测试用例2：固定时间 1000-01-01 12:00:00 转 UTC =====");
        Timestamp fixedCnTimestamp = Timestamp.valueOf("1000-01-01 12:00:00");
        convertAndPrint(fixedCnTimestamp);
    }

    /**
     * 核心转换方法（完全复刻你业务代码逻辑）
     */
    private static void convertAndPrint(Timestamp value) {
        System.out.println("原始 Timestamp（东八区）: " + value);

        // ========== 你业务中的核心代码 ==========
        Instant instant = Instant.ofEpochMilli(value.getTime());
        Object result = instant.atZone(ZoneOffset.UTC);
        // =======================================

        // 输出结果
        ZonedDateTime utcTime = (ZonedDateTime) result;
        System.out.println("转换后 UTC 时间: " + utcTime);

        // 验证时区是否正确
        System.out.println("最终时区: " + utcTime.getZone());
    }
}