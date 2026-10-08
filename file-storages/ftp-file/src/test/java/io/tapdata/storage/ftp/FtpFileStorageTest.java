package io.tapdata.storage.ftp;

import io.tapdata.file.TapFile;
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
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    void getFileTreatsDirectoryWithOneChildAsDirectory() throws Exception {
        client.singleChildDirectory = "/dir";

        TapFile file = storage.getFile("/dir");

        assertEquals(TapFile.TYPE_DIRECTORY, file.getType());
        assertEquals("dir", file.getName());
        assertEquals("/dir", file.getPath());
    }

    @Test
    void isFileExistTreatsDirectoryWithOneChildAsNotFile() throws Exception {
        client.singleChildDirectory = "/dir";

        assertFalse(storage.isFileExist("/dir"));
    }

    @Test
    void ftpSslIsRejectedUntilFtpsIsImplemented() throws Exception {
        FtpConfig config = new FtpConfig();
        config.setFtpSsl(true);
        setField(storage, "ftpConfig", config);

        UnsupportedOperationException exception = assertThrows(
                UnsupportedOperationException.class, storage::validateConfig);

        assertTrue(exception.getMessage().contains("ftpSsl=false"));
        assertTrue(exception.getMessage().contains("FTPS-capable connector"));
    }

    @Test
    void consumerCanReenterStorageWithoutDeadlock() throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                storage.readFile("/source.txt", inputStream -> {
                    try {
                        storage.getFile("/source.txt");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            } catch (Throwable e) {
                failure.set(e);
            }
        });
        worker.setDaemon(true);
        worker.start();

        worker.join(1000);

        assertFalse(worker.isAlive());
        assertNull(failure.get());
    }

    @Test
    void destroyClosesBlockedCallbackReadStream() throws Exception {
        CountDownLatch consumerStarted = new CountDownLatch(1);
        CountDownLatch allowConsumerFinish = new CountDownLatch(1);
        AtomicReference<Throwable> readerFailure = new AtomicReference<>();
        Thread reader = new Thread(() -> {
            try {
                storage.readFile("/source.txt", inputStream -> {
                    consumerStarted.countDown();
                    try {
                        if (!allowConsumerFinish.await(5, TimeUnit.SECONDS)) {
                            throw new RuntimeException("consumer was not released");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                });
            } catch (Throwable throwable) {
                readerFailure.set(throwable);
            }
        });
        reader.setDaemon(true);
        reader.start();
        assertTrue(consumerStarted.await(1, TimeUnit.SECONDS));

        Thread destroyer = new Thread(() -> {
            try {
                storage.destroy();
            } catch (IOException ignored) {
                // The recording client is not connected; completion is the behavior under test.
            }
        });
        destroyer.setDaemon(true);
        destroyer.start();

        try {
            destroyer.join(1000);
            assertFalse(destroyer.isAlive());
            assertTrue(client.disconnectCalled);
            assertFalse(client.completePendingCommandCalled);
        } finally {
            allowConsumerFinish.countDown();
            reader.join(1000);
            destroyer.join(1000);
        }
        assertFalse(reader.isAlive());
        assertNull(readerFailure.get());
    }

    @Test
    void destroyClosesUnclosedRawReadStream() throws Exception {
        InputStream stream = storage.readFile("/source.txt");
        Thread destroyer = new Thread(() -> {
            try {
                storage.destroy();
            } catch (IOException ignored) {
                // The recording client is not connected; completion is the behavior under test.
            }
        });
        destroyer.setDaemon(true);
        destroyer.start();

        try {
            destroyer.join(1000);
            assertFalse(destroyer.isAlive());
            assertTrue(client.disconnectCalled);
            assertFalse(client.completePendingCommandCalled);
        } finally {
            stream.close();
            destroyer.join(1000);
        }
    }

    @Test
    void destroyDoesNotWaitForPendingCommandOnActiveStream() throws Exception {
        InputStream stream = storage.readFile("/source.txt");
        client.blockCompletePendingCommand = true;
        Thread destroyer = new Thread(() -> {
            try {
                storage.destroy();
            } catch (IOException ignored) {
                // The recording client is not connected; completion is the behavior under test.
            }
        });
        destroyer.setDaemon(true);
        destroyer.start();

        try {
            destroyer.join(1000);
            assertFalse(destroyer.isAlive());
            assertTrue(client.disconnectCalled);
            assertFalse(client.completePendingCommandCalled);
        } finally {
            client.allowCompletePendingCommand.countDown();
            stream.close();
            destroyer.join(1000);
        }
    }

    @Test
    void streamRegistrationStopsAfterLifecycleCloseBegins() throws Exception {
        setField(storage, "destroying", true);
        Method registerActiveStream = FtpFileStorage.class
                .getDeclaredMethod("registerActiveStream", AutoCloseable.class);
        registerActiveStream.setAccessible(true);

        assertFalse((Boolean) registerActiveStream.invoke(storage, (AutoCloseable) () -> { }));
    }

    @Test
    void managedStreamUsesAtomicCloseGuard() throws Exception {
        Class<?> managedInputStream = Class.forName(
                "io.tapdata.storage.ftp.FtpFileStorage$ManagedInputStream");
        Field closed = managedInputStream.getDeclaredField("closed");

        assertEquals(java.util.concurrent.atomic.AtomicBoolean.class, closed.getType());
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static final class RecordingFtpClient extends FTPClient {
        private boolean completePendingCommandCalled;
        private boolean blockCompletePendingCommand;
        private boolean disconnectCalled;
        private final CountDownLatch allowCompletePendingCommand = new CountDownLatch(1);
        private IOException outputStreamCloseFailure;
        private String renamedSource;
        private String renamedTarget;
        private String singleChildDirectory;

        @Override
        public FTPFile[] listFiles(String pathname) {
            if (singleChildDirectory != null && singleChildDirectory.equals(pathname)) {
                FTPFile child = new FTPFile();
                child.setType(FTPFile.FILE_TYPE);
                child.setName("child.txt");
                child.setSize(4);
                return new FTPFile[]{child};
            }
            FTPFile file = new FTPFile();
            file.setType(FTPFile.FILE_TYPE);
            file.setName(pathname.substring(pathname.lastIndexOf('/') + 1));
            file.setSize(4);
            return new FTPFile[]{file};
        }

        @Override
        public String printWorkingDirectory() {
            return "/";
        }

        @Override
        public boolean changeWorkingDirectory(String pathname) {
            return singleChildDirectory != null && singleChildDirectory.equals(pathname);
        }

        @Override
        public InputStream retrieveFileStream(String remote) {
            return new ByteArrayInputStream("data".getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public boolean completePendingCommand() throws IOException {
            completePendingCommandCalled = true;
            if (blockCompletePendingCommand) {
                try {
                    allowCompletePendingCommand.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while waiting for pending command", e);
                }
            }
            return true;
        }

        @Override
        public void disconnect() {
            disconnectCalled = true;
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
