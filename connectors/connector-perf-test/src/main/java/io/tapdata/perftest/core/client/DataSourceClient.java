package io.tapdata.perftest.core.client;

import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;

import java.util.List;

/**
 * 统一数据源访问接口。
 * 所有 tapdata-connector 的性能测试均通过此接口驱动，与具体存储实现完全解耦。
 *
 * 新增数据源只需实现此接口 + 对应 DataSourceConfig，无需修改测试引擎。
 */
public interface DataSourceClient extends AutoCloseable {

    /** 初始化连接（对应 connector.onStart()）。必须在其他方法之前调用。 */
    void init() throws Exception;

    /**
     * 建表。若表已存在，实现类应先删除再创建（保证测试幂等性）。
     */
    void createTable(TapTable tapTable) throws Exception;

    /** 删除/清空表。 */
    void dropTable(String tableName) throws Exception;

    /**
     * 批量写入。
     *
     * @param events TapRecordEvent 列表（INSERT / UPDATE / DELETE）
     * @return WriteResult 含 inserted/updated/deleted 计数和错误数
     */
    WriteResult write(List<TapRecordEvent> events) throws Exception;

    /**
     * 批量读取一次（快照读）。
     *
     * @param table     目标表定义
     * @param batchSize 单批最大行数
     * @return ReadResult 含总行数和耗时
     * @throws UnsupportedOperationException 若此数据源不支持读取（如 Paimon 仅写）
     */
    ReadResult batchRead(TapTable table, int batchSize) throws Exception;

    /** 数据源类型标识，用于报告输出（如 "paimon", "mysql", "kafka"）。 */
    String getDataSourceType();

    /** 关闭所有资源（对应 connector.onStop()）。由 try-with-resources 自动调用。 */
    @Override
    void close() throws Exception;
}
