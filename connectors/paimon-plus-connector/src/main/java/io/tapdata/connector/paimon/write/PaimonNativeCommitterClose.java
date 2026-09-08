package io.tapdata.connector.paimon.write;

import io.tapdata.connector.paimon.service.PaimonStopResources;
import org.apache.paimon.table.sink.TableCommitImpl;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/** 原生维护主执行器的退出屏障；不执行维护，也不证明全局线程池中的异常派生任务退出。 */
public final class PaimonNativeCommitterClose implements AutoCloseable {
    private final AutoCloseable delegate;
    private final PaimonStopResources.Scope scope;
    private final ExecutorService executor;
    private boolean attempted;
    private Throwable failure;

    public PaimonNativeCommitterClose(AutoCloseable delegate, PaimonStopResources.Scope scope) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.scope = Objects.requireNonNull(scope, "scope");
        if (!(delegate instanceof TableCommitImpl)) {
            throw new IllegalArgumentException("Paimon committer type drift: expected TableCommitImpl");
        }
        this.executor = Objects.requireNonNull(((TableCommitImpl) delegate).getMaintainExecutor(),
                "Paimon maintenance executor");
    }

    @Override
    public synchronized void close() throws Exception {
        if (!attempted) {
            attempted = true;
            try {
                // 1.3.2 的 close 先关闭底层 commit，再 shutdownNow，且不等待维护线程退出。
                // 使用公开 getter 取得此 committer 的执行器，先排空已提交主任务再调用原生 close。
                // shutdown 不增加任务；禁止提交探测任务、维护重试或操作 Paimon 全局线程池。
                // https://github.com/apache/paimon/blob/release-1.3.2/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java#L398-L410
                scope.run("shutdown maintenance submissions", executor::shutdown);
                while (!executor.isTerminated()) {
                    scope.check("await maintenance termination");
                    executor.awaitTermination(Math.min(TimeUnit.MILLISECONDS.toNanos(50),
                            scope.controller().remainingNanos()), TimeUnit.NANOSECONDS);
                }
                scope.run("close native committer after maintenance", delegate::close);
                scope.check("publish native committer close proof");
            } catch (Exception | Error caught) {
                failure = caught;
                if (caught instanceof InterruptedException) { Thread.currentThread().interrupt(); }
            }
        }
        // 首次失败永久保留，重复 close 不重试原生操作，也不能伪造成功。
        if (failure instanceof Error) { throw (Error) failure; }
        if (failure != null) { throw (Exception) failure; }
    }
}
