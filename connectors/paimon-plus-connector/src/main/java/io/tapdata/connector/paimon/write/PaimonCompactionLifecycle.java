package io.tapdata.connector.paimon.write;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/** 单表唯一 Compaction 执行器的所有权和实际终止屏障。 */
public final class PaimonCompactionLifecycle {
    private final PaimonCompactionExecutor executor;

    private PaimonCompactionLifecycle(PaimonCompactionExecutor executor) { this.executor = executor; }

    public static PaimonCompactionLifecycle forTable(String tableKey) {
        return new PaimonCompactionLifecycle(new PaimonCompactionExecutor(Objects.requireNonNull(tableKey)));
    }

    /** 仅供没有异步生产者的直接测试策略；生产 Factory 始终持有真实 executor。 */
    public static PaimonCompactionLifecycle withoutCompactionExecutor() {
        return new PaimonCompactionLifecycle(null);
    }

    public ExecutorService compactionExecutor() {
        return Objects.requireNonNull(executor, "This lifecycle owns no Compaction executor");
    }

    public void auditControlFailure() { if (executor != null) { executor.audit(); } }
    public void sealFinalPrepare() { if (executor != null) { executor.auditAndSeal(); } }

    /**
     * graceful shutdown 后一直等到实际 termination；5 秒只是观察间隔，中断保存为硬失败。
     * Paimon 1.3.2 withCompactExecutor 将 closeCompactExecutorWhenLeaving 设为 false，
     * 原生 close 不关闭注入的执行器。Future.cancel/get/isDone 均不能证明工作线程已经退出；
     * 只有此屏障完成后才能调用会递归删除 Spill 目录的 IOManager.close。
     * https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java#L147
     * https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/disk/FileChannelManagerImpl.java#L125
     */
    public InterruptedException shutdownAndAwaitCompletion() {
        if (executor == null) { return null; }
        executor.shutdown();
        InterruptedException interruption = null;
        while (true) {
            try {
                if (executor.awaitTermination(5, TimeUnit.SECONDS)) { return interruption; }
            } catch (InterruptedException failure) {
                if (interruption == null) { interruption = failure; }
            }
        }
    }
}
