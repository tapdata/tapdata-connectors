package io.tapdata.connector.paimon.write;

import java.util.Objects;
import io.tapdata.connector.paimon.service.PaimonStopController;
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

    public void attachStopController(PaimonStopController controller) {
        if (executor != null) { executor.attachStopController(controller); }
    }

    public void beginFinal(PaimonStopController.FinalAttempt attempt) {
        if (!attempt.belongsTo(this)) { throw new IllegalArgumentException("Foreign lifecycle"); }
        if (executor != null) { executor.beginFinal(attempt); }
    }
    public void cancelForStop(PaimonStopController.FinalAttempt attempt) {
        if (!attempt.belongsTo(this)) { throw new IllegalArgumentException("Foreign lifecycle"); }
        if (executor != null) { executor.cancelForStop(attempt); }
    }
    public boolean isStopCancellation(Throwable failure, PaimonStopController.FinalAttempt attempt) {
        return executor != null && executor.isStopCancellation(failure, attempt);
    }
    public boolean isTerminated() { return executor == null || executor.isTerminated(); }
    public void shutdown() { if (executor != null) { executor.shutdown(); } }
    public boolean awaitTermination(long nanos) throws InterruptedException {
        return executor == null || executor.awaitTermination(nanos, TimeUnit.NANOSECONDS);
    }

    public void auditControlFailure() { if (executor != null) { executor.audit(); } }
    public void sealFinalPrepare() { if (executor != null) { executor.auditAndSeal(); } }

}
