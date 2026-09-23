package io.tapdata.connector.paimon.write;

import org.apache.paimon.CoreOptions.ExpireExecutionMode;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.operation.FileStoreCommit;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.utils.ThreadPoolUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/** 原版生命周期反例；通过表示复现 B1，不表示连接器已修复 B1。 */
class PaimonMaintenanceBoundaryTest {
    @ParameterizedTest
    @EnumSource(ExpireExecutionMode.class)
    void nativeParentTerminationAndCloseDoNotJoinFailedMaintenanceSibling(
            ExpireExecutionMode mode) throws Exception {
        ExecutorService children = Executors.newFixedThreadPool(2);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        Runnable expiration = () -> ThreadPoolUtils.randomlyOnlyExecute(children, index -> {
            if (index == 0) {
                await(entered);
                throw new IllegalStateException("测试注入：首个维护子任务失败");
            }
            entered.countDown();
            try {
                await(release);
            } finally {
                exited.countDown();
            }
        }, Arrays.asList(0, 1));
        // 使用真实公开提交入口触发原生 maintain，不反射调用或修改内核状态。
        // https://github.com/apache/paimon/blob/release-1.3.2/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java
        // https://github.com/apache/paimon/blob/release-1.3.2/paimon-api/src/main/java/org/apache/paimon/utils/ThreadPoolUtils.java
        TableCommitImpl committer = new TableCommitImpl(mock(FileStoreCommit.class), expiration,
                null, null, null, null, mode, "maintenance-boundary", false, 2);
        try {
            committer.commitMultiple(Collections.singletonList(new ManifestCommittable(1L)), false);
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            committer.getMaintainExecutor().shutdown();
            assertTrue(committer.getMaintainExecutor().awaitTermination(5, TimeUnit.SECONDS));
            assertEquals(1L, exited.getCount(), "主任务已退出，兄弟任务仍在执行");
            committer.close();
            assertEquals(1L, exited.getCount(), "原生 close 未等待兄弟任务");
        } finally {
            release.countDown();
            children.shutdownNow();
            committer.getMaintainExecutor().shutdownNow();
            assertTrue(children.awaitTermination(5, TimeUnit.SECONDS));
            assertTrue(committer.getMaintainExecutor().awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void rejectedSubmissionOrInterruptedWaitDoesNotStopAlreadySubmittedChild(boolean interrupt)
            throws Exception {
        ExecutorService children = new java.util.concurrent.ThreadPoolExecutor(1, 1, 0L,
                TimeUnit.MILLISECONDS, new java.util.concurrent.SynchronousQueue<>());
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch exited = new CountDownLatch(1);
        Runnable expiration = () -> ThreadPoolUtils.randomlyOnlyExecute(children, index -> {
            entered.countDown();
            try {
                await(release);
            } finally {
                exited.countDown();
            }
        }, interrupt ? Collections.singletonList(0) : Arrays.asList(0, 1));
        TableCommitImpl committer = new TableCommitImpl(mock(FileStoreCommit.class), expiration,
                null, null, null, null, ExpireExecutionMode.ASYNC, "maintenance-cancellation", false, 2);
        try {
            committer.commitMultiple(Collections.singletonList(new ManifestCommittable(1L)), false);
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            if (interrupt) {
                committer.getMaintainExecutor().shutdownNow();
            } else {
                // 一个 worker 且无队列：第二个 submit 被拒绝，首个工作已被接受。
                committer.getMaintainExecutor().shutdown();
            }
            assertTrue(committer.getMaintainExecutor().awaitTermination(5, TimeUnit.SECONDS));
            assertEquals(1L, exited.getCount(), "中断父任务或后续提交失败不终止已接受的子任务");
            committer.close();
            assertEquals(1L, exited.getCount());
        } finally {
            release.countDown();
            children.shutdownNow();
            committer.getMaintainExecutor().shutdownNow();
            assertTrue(children.awaitTermination(5, TimeUnit.SECONDS));
            assertTrue(committer.getMaintainExecutor().awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(15, TimeUnit.SECONDS)) {
                throw new AssertionError("测试屏障等待超时");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
