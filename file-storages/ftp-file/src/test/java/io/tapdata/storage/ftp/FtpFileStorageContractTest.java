package io.tapdata.storage.ftp;

import io.tapdata.file.TapFile;
import org.apache.commons.net.ftp.FTPClient;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.*;

class FtpFileStorageContractTest {

    @Test
    void ftpStorageDeclaresAtomicRenameAndDirectoryCapabilities() throws Exception {
        Object storage = new FtpFileStorage();
        java.lang.reflect.Method method = storage.getClass().getMethod("capabilities");
        assertEquals(EnumSet.of(
                io.tapdata.file.operation.FileStorageCapability.ATOMIC_RENAME,
                io.tapdata.file.operation.FileStorageCapability.MAKE_DIRECTORY
        ), method.invoke(storage));
    }

    @Test
    void saveFileFailsWhenFtpStoreFileReturnsFalse() throws Exception {
        TestStorage storage = new TestStorage();
        RecordingFtpClient client = new RecordingFtpClient();
        client.storeResult = false;
        setField(storage, "ftpClient", client);
        FtpConfig config = new FtpConfig();
        config.setEncoding("UTF-8");
        setField(storage, "ftpConfig", config);

        java.io.IOException exception = assertThrows(java.io.IOException.class, () ->
                storage.saveFile("orders/a.txt", new ByteArrayInputStream(new byte[]{1}), false));
        assertTrue(exception.getMessage().contains("storeFile"));
    }

    @Test
    void moveUsesFtpRenameResult() throws Exception {
        FtpFileStorage storage = new FtpFileStorage();
        RecordingFtpClient client = new RecordingFtpClient();
        client.renameResult = true;
        setField(storage, "ftpClient", client);
        FtpConfig config = new FtpConfig();
        config.setEncoding("UTF-8");
        setField(storage, "ftpConfig", config);

        assertTrue(storage.move("orders/a.txt", "archive/a.txt"));
        assertEquals("orders/a.txt", client.renameFrom);
        assertEquals("archive/a.txt", client.renameTo);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = FtpFileStorage.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class TestStorage extends FtpFileStorage {
        @Override
        public boolean isFileExist(String path) { return false; }

        @Override
        public TapFile getFile(String path) { return new TapFile().path(path).length(1L); }
    }

    private static class RecordingFtpClient extends FTPClient {
        private boolean storeResult;
        private boolean renameResult;
        private String renameFrom;
        private String renameTo;

        @Override
        public boolean changeWorkingDirectory(String pathname) { return true; }

        @Override
        public boolean storeFile(String remote, java.io.InputStream local) { return storeResult; }

        @Override
        public boolean rename(String from, String to) {
            renameFrom = from;
            renameTo = to;
            return renameResult;
        }
    }
}
