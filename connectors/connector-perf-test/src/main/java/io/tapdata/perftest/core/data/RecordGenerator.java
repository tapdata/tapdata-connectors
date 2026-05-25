package io.tapdata.perftest.core.data;

import java.util.Map;

/**
 * 记录生成器接口。
 * 实现类负责生成符合目标表 Schema 的测试数据（Map<String, Object>）。
 */
public interface RecordGenerator {

    /** 生成下一条记录。调用方应先检查 hasMore()。 */
    Map<String, Object> nextRecord();

    /** 是否还有未生成的记录。 */
    boolean hasMore();

    /** 重置生成器（可重复使用，如 Drill Test 中循环生成）。 */
    void reset();

    /** 计划生成的总记录数（0 表示无限制，用于 Drill Test）。 */
    long totalCount();
}
