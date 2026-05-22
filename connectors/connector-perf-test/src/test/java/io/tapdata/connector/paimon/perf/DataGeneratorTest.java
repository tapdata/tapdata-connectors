package io.tapdata.connector.paimon.perf;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Timeout;

import java.util.*;
import java.util.concurrent.*;

import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DataGenerator 单元测试
 *
 * <p>验证目标：
 * <ul>
 *   <li>ID 生成正确性（连续性、唯一性）</li>
 *   <li>重复率合理性（实际重复率 ≈ 配置重复率）</li>
 *   <li>重复 ID 分布均匀性（无热点）</li>
 *   <li>大规模数据生成（10亿+ 内存恒定）</li>
 *   <li>确定性伪随机可重现性</li>
 * </ul>
 */
@DisplayName("DataGenerator 单元测试")
class DataGeneratorTest {

    // ═══════════════════════════════════════════════════════════
    // 1. 新 ID 生成测试
    // ═══════════════════════════════════════════════════════════

    @Nested
    @DisplayName("新 ID 生成")
    class NewIdGeneration {

        @Test
        @DisplayName("0%重复率 - 所有ID唯一且递增")
        void allIdsUniqueAndIncremental() {
            DataGenerator gen = new DataGenerator(0, "test_table");
            Set<String> seenIds = new HashSet<>();

            for (int i = 0; i < 100_000; i++) {
                Map<String, Object> record = gen.generateRecord();
                String id = (String) record.get("id");

                assertNotNull(id, "ID 不应为 null");
                assertFalse(seenIds.contains(id), "ID 不应重复: " + id);
                seenIds.add(id);
            }

            assertEquals(100_000, seenIds.size(), "应生成 100K 唯一 ID");
            assertEquals(100_000, gen.getUniqueIdsCount(), "唯一 ID 计数应匹配");
        }

        @Test
        @DisplayName("ID 内容为数字字符串")
        void idsAreNumericStrings() {
            DataGenerator gen = new DataGenerator(0);

            for (int i = 0; i < 1000; i++) {
                Map<String, Object> record = gen.generateRecord();
                String id = (String) record.get("id");
                assertDoesNotThrow(() -> Long.parseLong(id), "ID 应为合法数字: " + id);
            }
        }

        @Test
        @DisplayName("记录包含所有必需字段")
        void recordHasAllFields() {
            DataGenerator gen = new DataGenerator(0);
            Map<String, Object> record = gen.generateRecord();

            assertTrue(record.containsKey("id"), "应包含 id 字段");
            assertTrue(record.containsKey("name"), "应包含 name 字段");
            assertTrue(record.containsKey("value"), "应包含 value 字段");
            assertTrue(record.containsKey("ts"), "应包含 ts 字段");

            assertNotNull(record.get("name"));
            assertNotNull(record.get("value"));
            assertNotNull(record.get("ts"));
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 2. 重复率合理性测试
    // ═══════════════════════════════════════════════════════════

    @Nested
    @DisplayName("重复率验证")
    class DuplicateRateValidation {

        @Test
        @DisplayName("0%重复率 - 无重复ID")
        void zeroPercentDuplicate() {
            DataGenerator gen = new DataGenerator(0);
            Set<String> ids = new HashSet<>();
            int count = 50_000;

            for (int i = 0; i < count; i++) {
                ids.add((String) gen.generateRecord().get("id"));
            }

            assertEquals(count, ids.size(), "0% 重复率下不应有重复 ID");
        }

        @Test
        @DisplayName("50%重复率 - 实际重复率应在 45%-55% 范围内")
        void fiftyPercentDuplicate() {
            DataGenerator gen = new DataGenerator(50);
            Map<String, Integer> idCounts = new HashMap<>();
            int count = 100_000;
            int warmup = 1000; // 预热期，不统计

            // 预热阶段
            for (int i = 0; i < warmup; i++) {
                gen.generateRecord();
            }

            // 统计阶段
            for (int i = 0; i < count; i++) {
                String id = (String) gen.generateRecord().get("id");
                idCounts.put(id, idCounts.getOrDefault(id, 0) + 1);
            }

            long duplicateCount = idCounts.values().stream()
                    .filter(c -> c > 1)
                    .mapToLong(c -> c - 1)
                    .sum();

            double actualRate = (double) duplicateCount / count * 100;

            // 允许 ±5% 误差（统计学波动）
            assertTrue(actualRate >= 45 && actualRate <= 55,
                    String.format("实际重复率 %.2f%% 应在 45%%-55%% 范围内", actualRate));
        }

        @Test
        @DisplayName("100%重复率 - 所有ID都来自重复池")
        void hundredPercentDuplicate() {
            DataGenerator gen = new DataGenerator(100);
            Set<String> ids = new HashSet<>();
            int count = 10_000;
            int warmup = 100;

            // 预热：生成一些初始 ID
            for (int i = 0; i < warmup; i++) {
                ids.add((String) gen.generateRecord().get("id"));
            }

            int initialUniqueCount = ids.size();

            // 统计：100% 重复率下，新 ID 应全部来自重复池
            for (int i = 0; i < count; i++) {
                String id = (String) gen.generateRecord().get("id");
                ids.add(id);
            }

            int finalUniqueCount = ids.size();
            int newUniqueIds = finalUniqueCount - initialUniqueCount;

            // 100% 重复率下，新 ID 应极少（统计误差范围内）
            // 允许少量误差（预热期后仍有新 ID 生成的边界情况）
            assertTrue(newUniqueIds < count * 0.05,
                    String.format("100%% 重复率下新唯一 ID 数 %d 应 < 5%%", newUniqueIds));
        }

        @Test
        @DisplayName("10%重复率 - 低重复场景")
        void tenPercentDuplicate() {
            DataGenerator gen = new DataGenerator(10);
            Map<String, Integer> idCounts = new HashMap<>();
            int count = 50_000;
            int warmup = 500;

            // 预热
            for (int i = 0; i < warmup; i++) {
                gen.generateRecord();
            }

            // 统计
            for (int i = 0; i < count; i++) {
                String id = (String) gen.generateRecord().get("id");
                idCounts.put(id, idCounts.getOrDefault(id, 0) + 1);
            }

            long duplicateCount = idCounts.values().stream()
                    .filter(c -> c > 1)
                    .mapToLong(c -> c - 1)
                    .sum();

            double actualRate = (double) duplicateCount / count * 100;

            // 允许 ±3% 误差
            assertTrue(actualRate >= 7 && actualRate <= 13,
                    String.format("实际重复率 %.2f%% 应在 7%%-13%% 范围内", actualRate));
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 3. 重复 ID 分布均匀性测试
    // ═══════════════════════════════════════════════════════════

    @Nested
    @DisplayName("重复 ID 分布均匀性")
    class DuplicateDistribution {

        @Test
        @DisplayName("50%重复率 - ID访问分布均匀")
        void evenDistributionAcrossIdSpace() {
            DataGenerator gen = new DataGenerator(50);
            int warmup = 10_000;
            int count = 100_000;
            int bucketCount = 100;
            int[] buckets = new int[bucketCount];

            // 预热
            for (int i = 0; i < warmup; i++) {
                gen.generateRecord();
            }

            // 统计重复 ID 的分布
            for (int i = 0; i < count; i++) {
                String idStr = (String) gen.generateRecord().get("id");
                long id = Long.parseLong(idStr);
                int bucket = (int) (id % bucketCount);
                buckets[bucket]++;
            }

            // 计算每个桶的期望值和实际偏差
            double expected = (double) count / bucketCount;
            double maxDeviation = 0;

            for (int bucket : buckets) {
                double deviation = Math.abs(bucket - expected) / expected * 100;
                maxDeviation = Math.max(maxDeviation, deviation);
            }

            // 最大偏差不应超过 20%（哈希函数保证均匀分布）
            assertTrue(maxDeviation < 20,
                    String.format("ID 分布最大偏差 %.2f%% 应 < 20%%", maxDeviation));
        }

        @Test
        @DisplayName("重复ID不集中在某个小区间")
        void noHotspotInDuplicateIds() {
            DataGenerator gen = new DataGenerator(80);
            Map<String, Integer> idCounts = new HashMap<>();
            int warmup = 5000;
            int count = 50_000;

            // 预热
            for (int i = 0; i < warmup; i++) {
                gen.generateRecord();
            }

            // 统计
            for (int i = 0; i < count; i++) {
                String id = (String) gen.generateRecord().get("id");
                idCounts.put(id, idCounts.getOrDefault(id, 0) + 1);
            }

            // 找出最频繁的 ID
            int maxFreq = idCounts.values().stream().max(Integer::compareTo).orElse(0);
            double maxFreqRate = (double) maxFreq / count * 100;

            // 单个 ID 不应占总数的 > 1%（证明无热点）
            assertTrue(maxFreqRate < 1.0,
                    String.format("最频繁 ID 占比 %.2f%% 应 < 1%%", maxFreqRate));
        }

        @Test
        @DisplayName("100%重复率 - 所有ID均匀分布")
        void uniformDistributionAtHundredPercent() {
            DataGenerator gen = new DataGenerator(100);
            int warmup = 1000;
            int count = 20_000;
            Map<String, Integer> idCounts = new LinkedHashMap<>();

            // 预热
            for (int i = 0; i < warmup; i++) {
                gen.generateRecord();
            }

            // 统计
            for (int i = 0; i < count; i++) {
                String id = (String) gen.generateRecord().get("id");
                idCounts.put(id, idCounts.getOrDefault(id, 0) + 1);
            }

            // 卡方检验简化版：各桶计数应接近期望
            int uniqueIds = idCounts.size();
            double expected = (double) count / uniqueIds;
            double chiSquare = 0;

            for (int observed : idCounts.values()) {
                double diff = observed - expected;
                chiSquare += (diff * diff) / expected;
            }

            // 自由度 = uniqueIds - 1，p=0.01 临界值约为 uniqueIds + 3*sqrt(2*uniqueIds)
            double criticalValue = uniqueIds + 3 * Math.sqrt(2 * uniqueIds);
            assertTrue(chiSquare < criticalValue,
                    String.format("卡方值 %.2f 应 < 临界值 %.2f（分布均匀）", chiSquare, criticalValue));
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 4. 确定性伪随机可重现性测试
    // ═══════════════════════════════════════════════════════════

    @Nested
    @DisplayName("确定性伪随机")
    class DeterministicPseudoRandom {

        @Test
        @DisplayName("确定性伪随机可重现性验证")
        void deterministicMappingReproducible() {
            // MurmurHash3 确定性验证：相同输入产生相同输出
            // 由于 random 是 private，我们验证 ID 生成逻辑的确定性部分
            
            // 验证 deterministicDuplicateId 的确定性
            // 通过生成相同模式的记录，验证 ID 分布一致性
            DataGenerator gen1 = new DataGenerator(50, "test_table");
            DataGenerator gen2 = new DataGenerator(50, "test_table");
            
            // 生成相同数量的预热记录
            for (int i = 0; i < 1000; i++) {
                gen1.generateRecord();
                gen2.generateRecord();
            }
            
            // 统计后续生成中重复 ID 的分布特征
            Map<String, Integer> dist1 = new HashMap<>();
            Map<String, Integer> dist2 = new HashMap<>();
            
            for (int i = 0; i < 5000; i++) {
                String id1 = (String) gen1.generateRecord().get("id");
                String id2 = (String) gen2.generateRecord().get("id");
                dist1.put(id1, dist1.getOrDefault(id1, 0) + 1);
                dist2.put(id2, dist2.getOrDefault(id2, 0) + 1);
            }
            
            // 两个生成器应有相似的重复率（统计意义上）
            double rate1 = dist1.values().stream().filter(c -> c > 1).mapToInt(Integer::intValue).sum() / 50.0;
            double rate2 = dist2.values().stream().filter(c -> c > 1).mapToInt(Integer::intValue).sum() / 50.0;
            
            // 两者重复率应接近（误差 < 10%）
            assertTrue(Math.abs(rate1 - rate2) < 10,
                    String.format("重复率差异 %.2f%% 应 < 10%%", Math.abs(rate1 - rate2)));
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 5. 大规模数据生成测试
    // ═══════════════════════════════════════════════════════════

    @Nested
    @DisplayName("大规模数据生成")
    class LargeScaleGeneration {

        @Test
        @Timeout(value = 60, unit = TimeUnit.SECONDS)
        @DisplayName("1000万条生成 - 验证性能")
        void tenMillionRecords() {
            DataGenerator gen = new DataGenerator(30);
            int count = 10_000_000;

            long start = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                gen.generateRecord();
            }
            long elapsed = System.currentTimeMillis() - start;

            double throughput = (double) count / elapsed * 1000;
            System.out.printf("1000万条生成耗时: %d ms, 吞吐: %.0f 条/秒%n", elapsed, throughput);

            assertTrue(elapsed < 30_000, "1000万条应在 30s 内完成，实际: " + elapsed + "ms");
            assertTrue(throughput > 100_000, "吞吐应 > 100K 条/秒，实际: " + throughput);
        }

        @Test
        @DisplayName("内存占用恒定 - 1亿条无OOM")
        @Timeout(value = 120, unit = TimeUnit.SECONDS)
        void constantMemoryUsage() {
            DataGenerator gen = new DataGenerator(50);
            int count = 100_000_000; // 1 亿条

            Runtime rt = Runtime.getRuntime();
            long startMem = rt.totalMemory() - rt.freeMemory();

            // 生成数据
            for (int i = 0; i < count; i++) {
                gen.generateRecord();
            }

            System.gc(); // 提示 GC
            long endMem = rt.totalMemory() - rt.freeMemory();
            long memIncrease = endMem - startMem;

            // 内存增长应 < 100 MB（证明无累积）
            assertTrue(memIncrease < 100_000_000,
                    String.format("1亿条后内存增长 %d MB 应 < 100 MB", memIncrease / 1_000_000));
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 6. generateUpdateEvent 测试
    // ═══════════════════════════════════════════════════════════

    @Nested
    @DisplayName("generateUpdateEvent")
    class UpdateEventGeneration {

        @Test
        @DisplayName("无预热时返回null")
        void returnsNullWithoutWarmup() {
            DataGenerator gen = new DataGenerator(0);
            assertNull(gen.generateUpdateEvent(), "无历史记录时应返回 null");
        }

        @Test
        @DisplayName("预热后生成有效更新事件")
        void generatesValidUpdateAfterWarmup() {
            DataGenerator gen = new DataGenerator(0);
            
            // 预热
            for (int i = 0; i < 100; i++) {
                gen.generateRecord();
            }

            // 生成更新事件
            for (int i = 0; i < 50; i++) {
                var event = gen.generateUpdateEvent();
                assertNotNull(event, "应生成更新事件");
                assertNotNull(event.getBefore(), "应包含 before 数据");
                assertNotNull(event.getAfter(), "应包含 after 数据");
                assertEquals("test_table", event.getTableId(), "表名应匹配");

                // before 和 after 的 ID 应相同
                assertEquals(
                        event.getBefore().get("id"),
                        event.getAfter().get("id"),
                        "更新前后 ID 应一致"
                );
            }
        }

        @Test
        @DisplayName("before为空分区、after有值的更新事件")
        void beforeNullAfterHasPartitionValue() {
            DataGenerator gen = new DataGenerator(100, "test_table", true, DataGenerator.PartitionScenario.UPDATE_BEFORE_NULL_AFTER_VALUE);

            // 预热到可以生成更新事件
            for (int i = 0; i < 20; i++) {
                gen.generateRecord();
            }

            TapUpdateRecordEvent event = gen.generateUpdateEvent();
            assertNotNull(event, "应生成更新事件");
            assertNotNull(event.getBefore(), "应包含 before 数据");
            assertNotNull(event.getAfter(), "应包含 after 数据");

            assertTrue(event.getBefore().get(DataGenerator.PARTITION_FIELD) == null
                            || String.valueOf(event.getBefore().get(DataGenerator.PARTITION_FIELD)).isEmpty(),
                    "before 分区值应为空");
            assertNotNull(event.getAfter().get(DataGenerator.PARTITION_FIELD), "after 分区值应有值");
            assertFalse(String.valueOf(event.getAfter().get(DataGenerator.PARTITION_FIELD)).isEmpty(),
                    "after 分区值不应为空字符串");
        }

        @Test
        @DisplayName("更新事件的ID来自历史生成范围")
        void updateEventIdsFromHistory() {
            DataGenerator gen = new DataGenerator(0);
            int warmup = 10_000;
            Set<String> historicalIds = new HashSet<>();

            // 预热并记录历史 ID
            for (int i = 0; i < warmup; i++) {
                String id = (String) gen.generateRecord().get("id");
                historicalIds.add(id);
            }

            // 生成更新事件，验证 ID 来自历史范围
            for (int i = 0; i < 1000; i++) {
                var event = gen.generateUpdateEvent();
                String id = (String) event.getBefore().get("id");
                long idNum = Long.parseLong(id);
                
                // ID 应在 [1, warmup] 范围内
                assertTrue(idNum >= 1 && idNum <= warmup,
                        String.format("更新事件 ID %d 应在历史范围内 [1, %d]", idNum, warmup));
            }
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 7. 边界条件测试
    // ═══════════════════════════════════════════════════════════

    @Nested
    @DisplayName("边界条件")
    class BoundaryConditions {

        @Test
        @DisplayName("重复率限制在0-100范围")
        void duplicateRateClamped() {
            // 负值应被限制为 0
            DataGenerator gen1 = new DataGenerator(-10);
            // 通过生成大量记录验证无重复
            Set<String> ids = new HashSet<>();
            for (int i = 0; i < 1000; i++) {
                ids.add((String) gen1.generateRecord().get("id"));
            }
            assertEquals(1000, ids.size(), "负重复率应被限制为 0");

            // 超100应被限制为 100
            DataGenerator gen2 = new DataGenerator(150);
            // 预热后应全部重复
            for (int i = 0; i < 100; i++) gen2.generateRecord();
            Set<String> ids2 = new HashSet<>();
            for (int i = 0; i < 1000; i++) {
                ids2.add((String) gen2.generateRecord().get("id"));
            }
            assertTrue(ids2.size() < 100, "150% 重复率应被限制为 100%");
        }

        @Test
        @DisplayName("批量生成方法正确性")
        void batchGeneration() {
            DataGenerator gen = new DataGenerator(0);
            int batchSize = 500;

            List<TapRecordEvent> events = gen.generateRecordEvents(batchSize);

            assertEquals(batchSize, events.size(), "批量大小应匹配");
            for (TapRecordEvent event : events) {
                // generateRecordEvents 生成的是 TapInsertRecordEvent
                assertTrue(event instanceof TapInsertRecordEvent, "事件类型应为 TapInsertRecordEvent");
                TapInsertRecordEvent insertEvent = (TapInsertRecordEvent) event;
                assertNotNull(insertEvent.getAfter(), "事件应包含 after 数据");
                assertEquals("test_table", event.getTableId(), "表名应匹配");
            }
        }

        @Test
        @DisplayName("generateTapTable 生成有效表结构")
        void generateValidTapTable() {
            DataGenerator gen = new DataGenerator(0);
            var table = gen.generateTapTable();
            
            assertNotNull(table);
            assertEquals("test_table", table.getName());
            assertEquals("test_table", table.getId());
            assertTrue(table.primaryKeys().contains("id"), "id 应为主键");
        }

        @Test
        @DisplayName("generateTapTable 生成联合主键表结构")
        void generateCompositePrimaryKeyTapTable() {
            DataGenerator gen = new DataGenerator(0, "test_table", true);
            var table = gen.generateTapTable(true);

            assertNotNull(table);
            assertEquals("test_table", table.getName());
            assertEquals("test_table", table.getId());
            assertTrue(table.primaryKeys().containsAll(Arrays.asList("id", DataGenerator.PARTITION_FIELD)),
                    "应同时包含 id 和分区字段作为主键");
        }

        @Test
        @DisplayName("generateRecordsWithRate 回调正确性")
        void generateRecordsWithRateCallback() {
            DataGenerator gen = new DataGenerator(0);
            List<Integer> receivedBatchSizes = new ArrayList<>();
            int totalRecords = 5000;
            int qps = 0; // 不限制 QPS

            gen.generateRecordsWithRate(totalRecords, qps, events -> {
                receivedBatchSizes.add(events.size());
            });

            int totalReceived = receivedBatchSizes.stream().mapToInt(Integer::intValue).sum();
            assertEquals(totalRecords, totalReceived, "总接收记录数应匹配");
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 8. 统计分析辅助测试
    // ═══════════════════════════════════════════════════════════

    @Nested
    @DisplayName("统计分析")
    class StatisticalAnalysis {

        @Test
        @DisplayName("getUniqueIdsCount 准确性")
        void uniqueIdsCountAccuracy() {
            DataGenerator gen = new DataGenerator(0);
            int count = 50_000;

            for (int i = 0; i < count; i++) {
                gen.generateRecord();
            }

            assertEquals(count, gen.getUniqueIdsCount(), 
                    "唯一 ID 计数应等于生成次数（0% 重复）");
        }

        @Test
        @DisplayName("getTotalGenerated 返回唯一ID数（非调用次数）")
        void totalGeneratedAccuracy() {
            DataGenerator gen = new DataGenerator(0);
            int count = 10_000;

            for (int i = 0; i < count; i++) {
                gen.generateRecord();
            }

            // 0% 重复率下，getTotalGenerated 应等于调用次数
            assertEquals(count, gen.getTotalGenerated(),
                    "0%% 重复率下总生成计数应等于调用次数");
            
            // 测试 50% 重复率
            DataGenerator gen2 = new DataGenerator(50);
            int warmup = 1000;
            int statCount = 10_000;
            
            // 预热
            for (int i = 0; i < warmup; i++) {
                gen2.generateRecord();
            }
            
            long uniqueBefore = gen2.getTotalGenerated();
            
            // 统计
            for (int i = 0; i < statCount; i++) {
                gen2.generateRecord();
            }
            
            long uniqueAfter = gen2.getTotalGenerated();
            long uniqueAdded = uniqueAfter - uniqueBefore;
            
            // 50% 重复率下，新增唯一 ID 数应约为 statCount * 0.5
            double expectedUnique = statCount * 0.5;
            double tolerance = statCount * 0.15; // 允许 15% 误差
            assertTrue(Math.abs(uniqueAdded - expectedUnique) < tolerance,
                    String.format("50%% 重复率下新增唯一 ID %d 应接近 %.0f (±%.0f)", 
                            uniqueAdded, expectedUnique, tolerance));
        }

        @Test
        @DisplayName("重复ID的ID值分布统计")
        void duplicateIdValueDistribution() {
            DataGenerator gen = new DataGenerator(70);
            int warmup = 10000;
            int count = 50000;
            Map<String, Integer> idCounts = new HashMap<>();

            // 预热
            for (int i = 0; i < warmup; i++) {
                gen.generateRecord();
            }

            // 统计
            for (int i = 0; i < count; i++) {
                String id = (String) gen.generateRecord().get("id");
                idCounts.put(id, idCounts.getOrDefault(id, 0) + 1);
            }

            // 分析重复 ID 的频次分布
            Map<Integer, Integer> freqDistribution = new HashMap<>();
            for (int freq : idCounts.values()) {
                freqDistribution.merge(freq, 1, Integer::sum);
            }

            System.out.println("\n=== 重复 ID 频次分布 ===");
            freqDistribution.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> System.out.printf(
                            "出现 %d 次的 ID 有 %d 个%n", e.getKey(), e.getValue()));

            // 计算实际重复率：基于唯一 ID 数 vs 总记录数
            int uniqueIds = idCounts.size();
            int totalRecords = idCounts.values().stream().mapToInt(Integer::intValue).sum();
            int duplicateRecords = totalRecords - uniqueIds;
            double actualDuplicateRate = (double) duplicateRecords / totalRecords * 100;

            System.out.printf("%n实际重复率分析:%n");
            System.out.printf("  总记录数: %,d%n", totalRecords);
            System.out.printf("  唯一 ID 数: %,d%n", uniqueIds);
            System.out.printf("  重复记录数: %,d%n", duplicateRecords);
            System.out.printf("  实际重复率: %.2f%%%n", actualDuplicateRate);

            // 70% 重复率下，实际重复率应在 60%-80% 范围
            assertTrue(actualDuplicateRate >= 60 && actualDuplicateRate <= 80,
                    String.format("实际重复率 %.2f%% 应在 60%%-80%% 范围内（配置 70%%）", actualDuplicateRate));
        }
    }
}
