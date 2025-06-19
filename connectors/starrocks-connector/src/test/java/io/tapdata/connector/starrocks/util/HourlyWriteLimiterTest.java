package io.tapdata.connector.starrocks.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

/**
 * 每小时写入限制器测试
 */
public class HourlyWriteLimiterTest {

    @Test
    @DisplayName("测试无限制模式")
    public void testNoLimit() {
        HourlyWriteLimiter limiter = new HourlyWriteLimiter(0);
        
        assertFalse(limiter.isLimitEnabled());
        assertTrue(limiter.canWrite(1000000000L)); // 1GB
        assertEquals(-1, limiter.getRemainingBytes());
        
        limiter.recordWrite(1000000000L);
        assertEquals(0, limiter.getCurrentHourWritten()); // 无限制时不记录
    }

    @Test
    @DisplayName("测试有限制模式")
    public void testWithLimit() {
        HourlyWriteLimiter limiter = new HourlyWriteLimiter(1); // 1GB限制
        
        assertTrue(limiter.isLimitEnabled());
        assertEquals(1024L * 1024L * 1024L, limiter.getHourlyLimitBytes());
        
        // 测试可以写入小量数据
        assertTrue(limiter.canWrite(100 * 1024 * 1024)); // 100MB
        
        // 记录写入
        limiter.recordWrite(100 * 1024 * 1024);
        assertEquals(100 * 1024 * 1024, limiter.getCurrentHourWritten());
        
        // 测试剩余容量
        long expected = 1024L * 1024L * 1024L - 100 * 1024 * 1024;
        assertEquals(expected, limiter.getRemainingBytes());
        
        // 测试超过限制
        assertFalse(limiter.canWrite(1024L * 1024L * 1024L)); // 1GB，会超过限制
    }

    @Test
    @DisplayName("测试null参数")
    public void testNullParameter() {
        HourlyWriteLimiter limiter = new HourlyWriteLimiter(null);
        
        assertFalse(limiter.isLimitEnabled());
        assertTrue(limiter.canWrite(1000000000L));
    }

    @Test
    @DisplayName("测试负数参数")
    public void testNegativeParameter() {
        HourlyWriteLimiter limiter = new HourlyWriteLimiter(-1);
        
        assertFalse(limiter.isLimitEnabled());
        assertTrue(limiter.canWrite(1000000000L));
    }

    @Test
    @DisplayName("测试距离下个小时的时间")
    public void testMinutesToNextHour() {
        HourlyWriteLimiter limiter = new HourlyWriteLimiter(1);
        
        long minutes = limiter.getMinutesToNextHour();
        assertTrue(minutes >= 0 && minutes < 60);
    }
}
