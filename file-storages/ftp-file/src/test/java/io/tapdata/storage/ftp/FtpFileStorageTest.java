package io.tapdata.storage.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FtpFileStorageTest {

    private FtpFileStorage storage;
    private RecordingFtpClient client;

    @BeforeEach
    void setUp() throws Exception {
        storage = new FtpFileStorage();
        client = new RecordingFtpClient();
        FtpConfig config = new FtpConfig();
        config.setEncoding(StandardCharsets.UTF_8.name());
        setField(storage, "ftpConfig", config);
        setField(storage, "ftpClient", client);
    }

    @Test
    void rawReadStreamCompletesPendingCommandWhenClosed() throws Exception {
        InputStream stream = storage.readFile("/source.txt");

        assertEquals('d', stream.read());
        stream.close();

        assertTrue(client.completePendingCommandCalled);
    }

    @Test
    void outputStreamCompletesPendingCommandEvenWhenDelegateCloseFails() throws Exception {
        client.outputStreamCloseFailure = new IOException("close failed");
        OutputStream stream = storage.openFileOutputStream("/target.txt", false);
        stream.write("data".getBytes(StandardCharsets.UTF_8));

        IOException exception = assertThrows(IOException.class, stream::close);

        assertEquals("close failed", exception.getMessage());
        assertTrue(client.completePendingCommandCalled);
    }

    @Test
    void moveUsesServerSideRename() throws Exception {
        assertTrue(storage.move("/source.txt", "/target.txt"));

        assertEquals("/source.txt", client.renamedSource);
        assertEquals("/target.txt", client.renamedTarget);
    }

    @Test
    void ftpSslIsRejectedUntilFtpsIsImplemented() throws Exception {
        FtpConfig config = new FtpConfig();
        config.setFtpSsl(true);
        setField(storage, "ftpConfig", config);

        assertThrows(UnsupportedOperationException.class, storage::validateConfig);
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static final class RecordingFtpClient extends FTPClient {
        private boolean completePendingCommandCalled;
        private IOException outputStreamCloseFailure;
        private String renamedSource;
        private String renamedTarget;

        @Override
        public FTPFile[] listFiles(String pathname) {
            FTPFile file = new FTPFile();
            file.setType(FTPFile.FILE_TYPE);
            file.setName(pathname.substring(pathname.lastIndexOf('/') + 1));
            file.setSize(4);
            return new FTPFile[]{file};
        }

        @Override
        public InputStream retrieveFileStream(String remote) {
            return new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public boolean completePendingCommand() {
            completePendingCommandCalled = true;
            return true;
        }

        @Override
        public OutputStream storeFileStream(String remote) {
            return outputStream();
        }

        @Override
        public OutputStream appendFileStream(String remote) {
            return outputStream();
        }

        private OutputStream outputStream() {
            return new ByteArrayOutputStream() {
                @Override
                public void close() throws IOException {
                    if (outputStreamCloseFailure != null) {
                        throw outputStreamCloseFailure;
                    }
                    super.close();
                }
            };
        }

        @Override
        public boolean rename(String from, String to) {
            renamedSource = from;
            renamedTarget = to;
            return true;
        }

        @Override
        public boolean isConnected() {
            return true;
        }
    }
}
