package io.tapdata.storage.sftp;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertFalse;

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

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
