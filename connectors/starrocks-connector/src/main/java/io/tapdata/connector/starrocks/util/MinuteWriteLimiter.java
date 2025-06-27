package io.tapdata.connector.starrocks.util;

import io.tapdata.kit.EmptyKit;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 每分钟写入限制器
 * 用于控制每分钟的数据写入量，防止超过配置的限制
 */
public class MinuteWriteLimiter {

    private final long minuteLimitBytes;
    private final AtomicLong currentMinuteWritten = new AtomicLong(0);
    private volatile int currentMinute = -1;
    
    /**
     * 构造函数
     * @param minuteLimitMB 每分钟限制的MB数，0表示不限制
     */
    public MinuteWriteLimiter(Integer minuteLimitMB) {
        if (EmptyKit.isNull(minuteLimitMB) || minuteLimitMB <= 0) {
            this.minuteLimitBytes = 0; // 0表示不限制
        } else {
            this.minuteLimitBytes = (long) minuteLimitMB * 1024 * 1024; // 转换为字节
        }
    }
    
    /**
     * 检查是否可以写入指定大小的数据
     * @param dataSize 要写入的数据大小（字节）
     * @return true表示可以写入，false表示超过限制需要等待
     */
    public boolean canWrite(long dataSize) {
        if (minuteLimitBytes == 0) {
            return true; // 不限制
        }

        int minute = LocalDateTime.now().getMinute();

        // 检查是否进入新的分钟
        if (minute != currentMinute) {
            synchronized (this) {
                if (minute != currentMinute) {
                    currentMinute = minute;
                    currentMinuteWritten.set(0);
                }
            }
        }

        // 检查写入后是否会超过限制
        long afterWrite = currentMinuteWritten.get() + dataSize;
        return afterWrite <= minuteLimitBytes;
    }
    
    /**
     * 记录已写入的数据大小
     * @param dataSize 已写入的数据大小（字节）
     */
    public void recordWrite(long dataSize) {
        if (minuteLimitBytes == 0) {
            return; // 不限制时不需要记录
        }

        currentMinuteWritten.addAndGet(dataSize);
    }

    /**
     * 获取当前分钟已写入的数据量（字节）
     * @return 已写入的字节数
     */
    public long getCurrentMinuteWritten() {
        return currentMinuteWritten.get();
    }

    /**
     * 获取当前分钟剩余可写入的数据量（字节）
     * @return 剩余可写入的字节数，-1表示不限制
     */
    public long getRemainingBytes() {
        if (minuteLimitBytes == 0) {
            return -1; // 不限制
        }

        return Math.max(0, minuteLimitBytes - currentMinuteWritten.get());
    }

    /**
     * 获取距离下一分钟的秒数
     * @return 距离下一分钟的秒数
     */
    public long getSecondsToNextMinute() {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime nextMinute = now.truncatedTo(ChronoUnit.MINUTES).plusMinutes(1);
        return ChronoUnit.SECONDS.between(now, nextMinute);
    }

    /**
     * 是否启用了限制
     * @return true表示启用了限制
     */
    public boolean isLimitEnabled() {
        return minuteLimitBytes > 0;
    }

    /**
     * 获取每分钟限制的字节数
     * @return 每分钟限制的字节数，0表示不限制
     */
    public long getMinuteLimitBytes() {
        return minuteLimitBytes;
    }
}
