package io.tapdata.perftest.core.data;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 默认记录生成器：生成 {id, name, value, ts} 四字段记录。
 *
 * 核心特性：
 * - O(1) 内存：使用 MurmurHash3 确定性伪随机生成重复 ID，无需存储历史记录池。
 * - 线程安全：AtomicLong 计数器，多线程共享同一实例时无竞争写丢失。
 * - 主键重复率：通过 pkDuplicateRate 控制 UPDATE 场景中的主键碰撞比例（0-100%）。
 */
public class DefaultRecordGenerator implements RecordGenerator {

    private final long  total;
    private final int   pkDuplicateRate;
    private final AtomicLong counter = new AtomicLong(0);

    public DefaultRecordGenerator(long total, int pkDuplicateRate) {
        this.total           = total;
        this.pkDuplicateRate = Math.max(0, Math.min(100, pkDuplicateRate));
    }

    @Override
    public Map<String, Object> nextRecord() {
        long seq = counter.getAndIncrement();
        long id  = (pkDuplicateRate > 0 && seq % 100 < pkDuplicateRate)
                   ? deterministicDupId(seq) : seq;
        Map<String, Object> r = new HashMap<>(4);
        r.put("id",    String.valueOf(id));
        r.put("name",  "user_" + (id % 100_000));
        r.put("value", (double)(id % 10_000) / 100.0);
        r.put("ts",    System.currentTimeMillis());
        return r;
    }

    @Override public boolean hasMore()    { return counter.get() < total; }
    @Override public void reset()         { counter.set(0); }
    @Override public long totalCount()    { return total; }

    /**
     * MurmurHash3 32-bit finalizer — 确定性生成重复 ID，O(1) 内存，无需存储历史。
     * 相同 seq 始终映射到相同的"旧主键"，便于调试复现。
     */
    private long deterministicDupId(long seq) {
        long h = seq ^ (seq >>> 16);
        h = (h * 0x45d9f3bL) & 0xFFFFFFFFL;
        h = h ^ (h >>> 16);
        long poolSize = Math.max(1L, total * pkDuplicateRate / 100);
        return h % poolSize;
    }
}
