package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.commit.PaimonAsyncCommitScheduler;
import io.tapdata.connector.paimon.commit.PaimonServiceLifecycle;
import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.logger.Log;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PaimonServicePhysicalTableOwnerTest {

    @Test
    void sameJvmMustRejectASecondOwnerUntilTheFirstOwnerReleasesTheTable() throws Exception {
        PaimonService first = service();
        PaimonService second = service();
        String tableKey = "default.orders";
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.location())
                .thenReturn(new Path("file:///tmp/paimon-owner-" + UUID.randomUUID()));

        Method register = method("registerPhysicalTableOwner", String.class, FileStoreTable.class);
        Method unregister = method("unregisterPhysicalTableOwner", String.class);

        try {
            register.invoke(first, tableKey, table);

            InvocationTargetException duplicate =
                    assertThrows(
                            InvocationTargetException.class,
                            () -> register.invoke(second, tableKey, table));
            assertInstanceOf(IllegalStateException.class, duplicate.getCause());

            unregister.invoke(first, tableKey);
            assertDoesNotThrow(() -> register.invoke(second, tableKey, table));
        } finally {
            unregister.invoke(first, tableKey);
            unregister.invoke(second, tableKey);
        }
    }

    @Test
    void closeTimeoutMustRetainOwnerUntilDeferredCleanupActuallyFinishes() throws Exception {
        PaimonService first = serviceWithCloseTimeout();
        PaimonService second = service();
        first.startForTest();
        String tableKey = "default.orders";
        FileStoreTable table = mock(FileStoreTable.class);
        when(table.location())
                .thenReturn(new Path("file:///tmp/paimon-owner-close-" + UUID.randomUUID()));
        Method register = method("registerPhysicalTableOwner", String.class, FileStoreTable.class);
        Method unregister = method("unregisterPhysicalTableOwner", String.class);
        PaimonServiceLifecycle lifecycle = lifecycle(first);
        PaimonServiceLifecycle.Ingress blockedIngress = lifecycle.enter("owner-close-timeout");
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread closer =
                new Thread(
                        () -> {
                            try {
                                first.close();
                            } catch (Throwable failure) {
                                closeFailure.set(failure);
                            }
                        },
                        "paimon-owner-close-timeout");
        closer.setDaemon(true);

        try {
            register.invoke(first, tableKey, table);
            closer.start();
            closer.join(2_000L);
            assertInstanceOf(IllegalStateException.class, closeFailure.get());

            InvocationTargetException stillOwned =
                    assertThrows(
                            InvocationTargetException.class,
                            () -> register.invoke(second, tableKey, table));
            assertInstanceOf(IllegalStateException.class, stillOwned.getCause());

            blockedIngress.close();
            awaitClosed(lifecycle);
            assertDoesNotThrow(() -> register.invoke(second, tableKey, table));
        } finally {
            blockedIngress.close();
            unregister.invoke(first, tableKey);
            unregister.invoke(second, tableKey);
        }
    }

    private static PaimonService service() {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        config.setBatchAccumulationSize(100);
        config.setCommitIntervalMs(30_000);
        config.setEnableAsyncCommit(false);
        return new PaimonService(config, mock(Log.class), () -> 100L, () -> { });
    }

    private static PaimonService serviceWithCloseTimeout() {
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        config.setBatchAccumulationSize(100);
        config.setCommitIntervalMs(30_000);
        config.setEnableAsyncCommit(false);
        return new PaimonService(
                config,
                mock(Log.class),
                () -> 100L,
                () -> { },
                PaimonAsyncCommitScheduler::newDaemonExecutor,
                200L,
                TimeUnit.MILLISECONDS);
    }

    private static PaimonServiceLifecycle lifecycle(PaimonService service) throws Exception {
        Field field = PaimonService.class.getDeclaredField("lifecycle");
        field.setAccessible(true);
        return (PaimonServiceLifecycle) field.get(service);
    }

    private static void awaitClosed(PaimonServiceLifecycle lifecycle) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (System.nanoTime() < deadline) {
            if (lifecycle.state() == PaimonServiceLifecycle.State.CLOSED) {
                return;
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("Timed out waiting for deferred close");
    }

    private static Method method(String name, Class<?>... parameterTypes) throws Exception {
        Method method = PaimonService.class.getDeclaredMethod(name, parameterTypes);
        method.setAccessible(true);
        return method;
    }
}
