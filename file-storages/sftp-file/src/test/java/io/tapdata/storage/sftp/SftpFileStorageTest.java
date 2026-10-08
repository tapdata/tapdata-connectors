package io.tapdata.storage.sftp;

import com.jcraft.jsch.ChannelSftp;
import io.tapdata.storage.kit.TransferableReentrantLock;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SftpFileStorageTest {

    @Test
    void streamRegistrationStopsAfterLifecycleCloseBegins() throws Exception {
        SftpFileStorage storage = new SftpFileStorage();
        setField(storage, "destroying", true);
        Method registerActiveStream = SftpFileStorage.class
                .getDeclaredMethod("registerActiveStream", AutoCloseable.class);
        registerActiveStream.setAccessible(true);

        assertFalse((Boolean) registerActiveStream.invoke(storage, (AutoCloseable) () -> { }));
    }

    @Test
    void destroyDisconnectsBeforeWaitingForAnActiveOperation() throws Exception {
        SftpFileStorage storage = new SftpFileStorage();
        AtomicReference<TransferableReentrantLock.Permit> activePermit = new AtomicReference<>();
        CountDownLatch disconnected = new CountDownLatch(1);
        ChannelSftp channel = new ChannelSftp() {
            @Override
            public void disconnect() {
                disconnected.countDown();
                activePermit.get().release();
            }
        };
        setField(storage, "channel", channel);
        Field lockField = SftpFileStorage.class.getDeclaredField("operationLock");
        lockField.setAccessible(true);
        TransferableReentrantLock operationLock = (TransferableReentrantLock) lockField.get(storage);
        activePermit.set(operationLock.acquireUninterruptibly());

        Thread destroyer = new Thread(storage::destroy);
        destroyer.setDaemon(true);
        destroyer.start();

        try {
            destroyer.join(1000);
            assertFalse(destroyer.isAlive());
            assertTrue(disconnected.await(1, TimeUnit.SECONDS));
        } finally {
            activePermit.get().release();
            destroyer.join(1000);
        }
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
