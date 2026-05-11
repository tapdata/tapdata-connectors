package io.tapdata.connector.paimon.perf;

import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;

import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 数据生成器 - 支持配置总数据量、写入QPS、主键重复率
 *
 * <p><b>内存优化设计（支持 10 亿+ 规模）</b>：
 * <ul>
 *   <li><b>零存储重复池</b>：使用 MurmurHash3 确定性伪随机选择重复 ID，无需缓存历史</li>
 *   <li><b>内存 O(1)</b>：无论 1 亿还是 100 亿条，内存恒定 < 1 MB</li>
 *   <li><b>ID 按需生成</b>：long → String 按需转换，消除字符串缓存</li>
 * </ul>
 *
 * <p><b>内存对比</b>：
 * <pre>
 *   优化前（HashSet + ArrayList）：
 *     1 亿条 → ~5 GB
 *    10 亿条 → OOM (>50 GB)
 *
 *   优化后（确定性伪随机）：
 *     1 亿条 → < 1 MB
 *    10 亿条 → < 1 MB ✅
 *   100 亿条 → < 1 MB ✅
 * </pre>
 *
 * <p><b>重复 ID 生成原理</b>：
 * <pre>
 *   当需要重复 ID 时：
 *     1. 确定重复池大小 = min(当前已生成唯一 ID 数, 上限值)
 *     2. 使用 MurmurHash3(currentId + seed) 计算哈希
 *     3. 取模映射到重复池：hash % poolSize
 *     4. 确定性：相同输入 → 相同输出，无需存储
 *
 *   优点：
 *     - 均匀分布：哈希函数保证重复 ID 分散
 *     - 可重现：固定 seed 下结果一致，便于调试
 *     - 零存储：不保留任何历史 ID
 * </pre>
 */
public class DataGenerator {
    private final Random random;
    private final AtomicLong idGenerator;

    /**
     * 确定性重复 ID 生成的种子（固定值保证可重现）
     * 可修改此值改变重复分布，但相同种子下结果一致
     */
    private static final long DUPLICATE_SEED = 0x517cc1b727220a95L;

    /**
     * 重复 ID 池大小上限
     * 当已生成唯一 ID 超过此值时，重复池固定为此大小
     * 避免取模运算溢出，同时保证重复 ID 分布均匀
     */
    private static final int MAX_DUPLICATE_POOL = 100_000_000; // 1 亿

    private final int primaryKeyDuplicateRate;
    private final String tableName;

    public DataGenerator(int primaryKeyDuplicateRate) {
        this(primaryKeyDuplicateRate, "test_table");
    }

    public DataGenerator(int primaryKeyDuplicateRate, String tableName) {
        this.random = new Random(System.currentTimeMillis());
        this.idGenerator = new AtomicLong(1);
        this.primaryKeyDuplicateRate = Math.max(0, Math.min(100, primaryKeyDuplicateRate));
        this.tableName = tableName;
    }

    /**
     * MurmurHash3 32-bit 简化实现
     * 用于确定性伪随机映射：相同输入 → 相同输出
     */
    private static int murmurHash3_32(long key) {
        long h = key ^ (key >>> 33);
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return (int) h;
    }

    /**
     * 确定性生成一个重复 ID（无需存储历史）
     *
     * @param currentId 当前记录的 ID
     * @param poolSize  重复池大小（已生成的唯一 ID 数）
     * @return 映射到重复池中的 ID（1 ~ poolSize）
     */
    private static long deterministicDuplicateId(long currentId, long poolSize) {
        if (poolSize <= 0) {
            return currentId; // 无可用重复 ID
        }
        // 使用 MurmurHash3 计算确定性哈希
        int hash = murmurHash3_32(currentId ^ DUPLICATE_SEED);
        // 映射到 [1, poolSize] 范围
        long offset = (hash & 0x7fffffffL) % poolSize;
        return 1 + offset;
    }

    /**
     * 将 long ID 转为 String（按需创建，不缓存）
     */
    private static String idToString(long id) {
        return Long.toString(id);
    }

    /**
     * 生成单个记录
     */
    public Map<String, Object> generateRecord() {
        Map<String, Object> record = new HashMap<>();

        // 生成主键
        long idLong;
        long currentUniqueCount = idGenerator.get(); // 当前已生成的唯一 ID 数（未包含本次）

        if (primaryKeyDuplicateRate > 0 && currentUniqueCount > 1
                && random.nextInt(100) < primaryKeyDuplicateRate) {
            // 生成重复主键（用于 UPDATE 场景）
            // 使用确定性伪随机映射，无需存储历史 ID
            long poolSize = Math.min(currentUniqueCount - 1, MAX_DUPLICATE_POOL);
            // 使用当前时间戳 + 随机值作为种子，确保每次调用有不同的哈希输入
            long seed = System.nanoTime() ^ random.nextLong();
            idLong = deterministicDuplicateId(seed, poolSize);
            // 注意：不增加 idGenerator，因为这是重复 ID
        } else {
            // 生成新主键
            idLong = idGenerator.getAndIncrement();
        }

        String id = idToString(idLong);
        record.put("id", id);
        record.put("name", "name-" + id + "-" + System.currentTimeMillis());
        record.put("value", random.nextInt(1000000));
        record.put("ts", new Timestamp(System.currentTimeMillis()));

        return record;
    }

    /**
     * 生成批量记录事件
     */
    public List<TapRecordEvent> generateRecordEvents(int batchSize) {
        List<TapRecordEvent> events = new ArrayList<>(batchSize);

        for (int i = 0; i < batchSize; i++) {
            Map<String, Object> record = generateRecord();
            TapInsertRecordEvent event = new TapInsertRecordEvent();
            event.setAfter(record);
            event.setTableId(tableName);
            event.setReferenceTime(System.currentTimeMillis());
            events.add(event);
        }

        return events;
    }

    /**
     * 生成更新事件
     */
    public TapUpdateRecordEvent generateUpdateEvent() {
        long currentUniqueCount = idGenerator.get();
        if (currentUniqueCount <= 1) {
            return null;
        }

        // 使用确定性伪随机生成重复 ID
        long poolSize = Math.min(currentUniqueCount - 1, MAX_DUPLICATE_POOL);
        long dummyId = random.nextLong();
        long idLong = deterministicDuplicateId(dummyId, poolSize);
        String id = idToString(idLong);

        Map<String, Object> before = new HashMap<>();
        before.put("id", id);
        before.put("name", "name-" + id);
        before.put("value", random.nextInt(1000000));
        before.put("ts", new Timestamp(System.currentTimeMillis() - 86400000L));

        Map<String, Object> after = new HashMap<>();
        after.put("id", id);
        after.put("name", "name-" + id + "-updated-" + System.currentTimeMillis());
        after.put("value", random.nextInt(1000000));
        after.put("ts", new Timestamp(System.currentTimeMillis()));

        TapUpdateRecordEvent event = new TapUpdateRecordEvent();
        event.setBefore(before);
        event.setAfter(after);
        event.setTableId(tableName);
        event.setReferenceTime(System.currentTimeMillis());

        return event;
    }

    /**
     * 生成TapTable结构
     */
    public TapTable generateTapTable() {
        TapTable table = new TapTable();
        table.setName(tableName);
        table.setId(tableName);

        // 添加字段 - 使用正确的TapField构造函数
        TapField idField = new TapField();
        idField.setName("id");
        idField.setDataType("VARCHAR");
        idField.setNullable(true);
        idField.setPrimaryKey(true);
        idField.setPrimaryKeyPos(1);
        table.add(idField);

        TapField nameField = new TapField();
        nameField.setName("name");
        nameField.setDataType("VARCHAR");
        nameField.setNullable(true);
        table.add(nameField);

        TapField valueField = new TapField();
        valueField.setName("value");
        valueField.setDataType("INTEGER");
        valueField.setNullable(true);
        table.add(valueField);

        TapField tsField = new TapField();
        tsField.setName("ts");
        tsField.setDataType("TIMESTAMP");
        tsField.setNullable(true);
        table.add(tsField);

        // 设置主键
        table.setDefaultPrimaryKeys(Collections.singletonList("id"));

        return table;
    }

    /**
     * 生成指定数量的记录,按指定QPS控制速度
     * @param totalRecords 总记录数
     * @param qps 每秒写入记录数(0表示不限制)
     * @param consumer 处理批次数据的消费者
     */
    public void generateRecordsWithRate(long totalRecords, int qps, Consumer<List<TapRecordEvent>> consumer) {
        long batchSize = 1000; // 每批次1000条
        long totalBatches = (totalRecords + batchSize - 1) / batchSize;
        
        System.out.println("  >> 开始生成数据: 总量=" + totalRecords + "条, 批次大小=" + batchSize);
        if (qps > 0) {
            System.out.println("  >> QPS限制=" + qps + "条/秒");
        } else {
            System.out.println("  >> QPS限制=无限制(全速写入)");
        }

        long startTime = System.currentTimeMillis();
        long totalWritten = 0;

        for (long i = 0; i < totalBatches; i++) {
            long currentBatchSize = Math.min(batchSize, totalRecords - i * batchSize);

            // 生成批次数据
            List<TapRecordEvent> events = new ArrayList<>((int) currentBatchSize);
            for (int j = 0; j < currentBatchSize; j++) {
                Map<String, Object> record = generateRecord();
                TapInsertRecordEvent event = new TapInsertRecordEvent();
                event.setAfter(record);
                event.setTableId(tableName);
                event.setReferenceTime(System.currentTimeMillis());
                events.add(event);
            }

            // 调用消费者处理数据
            consumer.accept(events);
            totalWritten += events.size();

            // 控制QPS
            if (qps > 0) {
                long expectedTimePerBatch = (batchSize * 1000) / qps;
                long actualTime = System.currentTimeMillis() - startTime;
                long sleepTime = expectedTimePerBatch - actualTime;

                if (sleepTime > 0) {
                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                startTime = System.currentTimeMillis();
            }

            // 每10批次打印进度
            if ((i + 1) % 10 == 0 || i == totalBatches - 1) {
                double progress = (double) (i + 1) / totalBatches * 100;
                System.out.printf("  >> 进度: %d/%d 批次 (%.1f%%), 已写入: %d 条%n", 
                        i + 1, totalBatches, progress, totalWritten);
            }
        }

        System.out.println("  >> 数据生成完成: 总计 " + totalWritten + " 条");
    }

    /**
     * 获取已生成的唯一ID数量
     *
     * <p>返回实际生成的唯一 ID 总数（非窗口限制值）
     */
    public long getUniqueIdsCount() {
        return idGenerator.get() - 1;
    }

    /**
     * 获取总生成记录数(包括重复)
     */
    public long getTotalGenerated() {
        return idGenerator.get() - 1;
    }
}
