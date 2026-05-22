package io.tapdata.perftest.core.scenario;

import io.tapdata.perftest.core.client.DataSourceClient;

/**
 * 测试场景接口。每个场景封装一种测试行为（写入、读取、混合、长时稳定性）。
 * 场景不依赖任何具体数据源，只通过 DataSourceClient 接口操作目标系统。
 */
public interface BenchmarkScenario {

    /** 场景名称，用于报告标题。 */
    String getName();

    /**
     * 执行场景并返回结果。
     *
     * @param client 已初始化的 DataSourceClient（由 BenchmarkEngine 负责生命周期管理）
     * @return ScenarioResult 执行结果快照
     */
    ScenarioResult execute(DataSourceClient client) throws Exception;
}
