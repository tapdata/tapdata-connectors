package io.tapdata.storage.ftp;

import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.storage.kit.FileMatchKit;
import io.tapdata.storage.kit.TransferableReentrantLock;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class FtpFileStorage implements TapFileStorage {

    private static final int DEFAULT_CONTROL_TIMEOUT_MILLIS = 60_000;
    private final TransferableReentrantLock ioLock = new TransferableReentrantLock();
    private final Object lifecycleMonitor = new Object();
    private final Set<AutoCloseable> activeStreams = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private volatile boolean destroying;
    private FtpConfig ftpConfig;
    private FTPClient ftpClient;

    private interface ForceCloseable {
        void closeForDestroy() throws IOException;
    }

    @Override
    public void init(Map<String, Object> params) throws IOException {
        closeActiveStreams(beginLifecycleClose());
        TransferableReentrantLock.Permit permit = ioLock.acquireUninterruptibly();
        try {
            FtpConfig config = new FtpConfig().load(params);
            validateConfig(config);
            closeClient(ftpClient);
            ftpClient = null;
            ftpConfig = config;

            FTPClient client = createFtpClient();
            try {
                client.setConnectTimeout(config.getFtpConnectTimeout());
                client.setDataTimeout(config.getFtpDataTimeout());
                client.setDefaultTimeout(config.getFtpConnectTimeout() > 0
                        ? config.getFtpConnectTimeout() : DEFAULT_CONTROL_TIMEOUT_MILLIS);
                client.setControlEncoding(config.getEncoding());
                client.connect(config.getFtpHost(), config.getFtpPort());
                if (!FTPReply.isPositiveCompletion(client.getReplyCode())) {
                    throw new IOException("connect to ftp server failed: " + client.getReplyString());
                }
                boolean loggedIn;
                if (config.getFtpAccount() != null && !config.getFtpAccount().trim().isEmpty()) {
                    loggedIn = client.login(config.getFtpUsername(), config.getFtpPassword(), config.getFtpAccount());
                } else {
                    loggedIn = client.login(config.getFtpUsername(), config.getFtpPassword());
                }
                if (!loggedIn || !FTPReply.isPositiveCompletion(client.getReplyCode())) {
                    throw new IOException("login to ftp server failed: " + client.getReplyString());
                }
                if (Boolean.TRUE.equals(config.getFtpPassiveMode())) {
                    client.enterLocalPassiveMode();
                } else {
                    client.enterLocalActiveMode();
                }
                if (!client.setFileType(FTP.BINARY_FILE_TYPE)) {
                    throw new IOException("set ftp binary file type failed: " + client.getReplyString());
                }
                ftpClient = client;
            } catch (Throwable throwable) {
                closeClient(client);
                ftpClient = null;
                if (throwable instanceof IOException) {
                    throw (IOException) throwable;
                }
                if (throwable instanceof RuntimeException) {
                    throw (RuntimeException) throwable;
                }
                throw new IOException("initialize ftp storage failed", throwable);
            }
        } finally {
            permit.release();
            endLifecycleClose();
        }
    }

    protected FTPClient createFtpClient() {
        return new FTPClient();
    }

    void validateConfig() {
        validateConfig(ftpConfig);
    }

    private void validateConfig(FtpConfig config) {
        if (config != null && Boolean.TRUE.equals(config.getFtpSsl())) {
            throw new UnsupportedOperationException(
                    "ftpSsl=true is not supported by ftp-file. Set ftpSsl=false for plain FTP, "
                            + "or use an FTPS-capable connector for TLS.");
        }
    }

    @Override
    public void destroy() throws IOException {
        Throwable failure = closeActiveStreams(beginLifecycleClose(), true);
        TransferableReentrantLock.Permit permit = ioLock.acquireUninterruptibly();
        try {
            failure = appendFailure(failure, closeClient(ftpClient, false));
            ftpClient = null;
            ftpConfig = null;
        } finally {
            permit.release();
            endLifecycleClose();
        }
        throwFailure(failure);
    }

    private AutoCloseable[] beginLifecycleClose() {
        synchronized (lifecycleMonitor) {
            destroying = true;
            return activeStreams.toArray(new AutoCloseable[0]);
        }
    }

    private void endLifecycleClose() {
        synchronized (lifecycleMonitor) {
            destroying = false;
        }
    }

    private TransferableReentrantLock.Permit acquireIoLock() throws IOException {
        if (destroying) {
            throw new IOException("ftp storage is being destroyed");
        }
        TransferableReentrantLock.Permit permit = ioLock.acquireUninterruptibly();
        if (destroying) {
            permit.release();
            throw new IOException("ftp storage is being destroyed");
        }
        return permit;
    }

    private Throwable closeActiveStreams(AutoCloseable[] streams) {
        return closeActiveStreams(streams, false);
    }

    private Throwable closeActiveStreams(AutoCloseable[] streams, boolean force) {
        Throwable failure = null;
        for (AutoCloseable stream : streams) {
            try {
                if (force && stream instanceof ForceCloseable) {
                    ((ForceCloseable) stream).closeForDestroy();
                } else {
                    stream.close();
                }
            } catch (Throwable throwable) {
                failure = appendFailure(failure, throwable);
            }
        }
        return failure;
    }

    private boolean registerActiveStream(AutoCloseable stream) {
        synchronized (lifecycleMonitor) {
            if (destroying) {
                return false;
            }
            activeStreams.add(stream);
            return true;
        }
    }

    private IOException closeClient(FTPClient client) {
        return closeClient(client, true);
    }

    private IOException closeClient(FTPClient client, boolean graceful) {
        if (client == null) {
            return null;
        }
        IOException failure = null;
        if (graceful) {
            try {
                if (client.isConnected()) {
                    client.logout();
                }
            } catch (IOException e) {
                failure = e;
            }
        }
        try {
            if (client.isConnected()) {
                client.disconnect();
            }
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        }
        return failure;
    }

    @Override
    public TapFile getFile(String path) throws IOException {
        TransferableReentrantLock.Permit permit = acquireIoLock();
        try {
            ensureReady();
            if (isDirectoryExistLocked(path)) {
                return toDirectory(path);
            }
            FTPFile file = singleFile(path);
            return file == null ? null : toTapFile(file, path);
        } finally {
            permit.release();
        }
    }

    private TapFile toTapFile(FTPFile file, String path) {
        TapFile tapFile = new TapFile();
        tapFile.type(file.isDirectory() ? TapFile.TYPE_DIRECTORY : TapFile.TYPE_FILE)
                .name(file.getName())
                .path(path)
                .length(file.getSize())
                .lastModified(file.getTimestamp() == null ? 0L : file.getTimestamp().getTimeInMillis());
        return tapFile;
    }

    private TapFile toDirectory(String path) {
        String normalizedPath = path.endsWith("/") && path.length() > 1
                ? path.substring(0, path.length() - 1) : path;
        TapFile tapFile = new TapFile();
        return tapFile.type(TapFile.TYPE_DIRECTORY)
                .name(normalizedPath.substring(normalizedPath.lastIndexOf('/') + 1))
                .path(path)
                .length(0L)
                .lastModified(0L);
    }

    @Override
    public void readFile(String path, Consumer<InputStream> consumer) throws IOException {
        TransferableReentrantLock.Permit permit = acquireIoLock();
        try {
            ensureReady();
            if (!isFileExistLocked(path)) {
                return;
            }
            InputStream inputStream = ftpClient.retrieveFileStream(encodeISO(path));
            if (inputStream == null) {
                throw new IOException("open ftp input stream failed: " + ftpClient.getReplyString());
            }
            ManagedInputStream managed = new ManagedInputStream(inputStream, permit);
            if (!managed.isRegistered()) {
                managed.close();
                throw new IOException("ftp storage is being destroyed");
            }
            Throwable failure = null;
            try {
                consumer.accept(managed);
            } catch (Throwable throwable) {
                failure = throwable;
            } finally {
                try {
                    managed.close();
                } catch (Throwable throwable) {
                    failure = appendFailure(failure, throwable);
                }
            }
            throwFailure(failure);
        } finally {
            permit.release();
        }
    }

    @Override
    public InputStream readFile(String path) throws IOException {
        TransferableReentrantLock.Permit permit = acquireIoLock();
        boolean ownershipTransferred = false;
        try {
            ensureReady();
            if (!isFileExistLocked(path)) {
                return null;
            }
            InputStream inputStream = ftpClient.retrieveFileStream(encodeISO(path));
            if (inputStream == null) {
                throw new IOException("open ftp input stream failed: " + ftpClient.getReplyString());
            }
            ManagedInputStream managed = new ManagedInputStream(inputStream, permit);
            if (!managed.isRegistered()) {
                managed.close();
                throw new IOException("ftp storage is being destroyed");
            }
            ownershipTransferred = true;
            return managed;
        } finally {
            if (!ownershipTransferred) {
                permit.release();
            }
        }
    }

    private class ManagedInputStream extends FilterInputStream implements ForceCloseable {
        private final TransferableReentrantLock.Permit permit;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final boolean registered;

        private ManagedInputStream(InputStream inputStream, TransferableReentrantLock.Permit permit) {
            super(inputStream);
            this.permit = permit;
            this.registered = registerActiveStream(this);
        }

        private boolean isRegistered() {
            return registered;
        }

        @Override
        public void close() throws IOException {
            close(true);
        }

        @Override
        public void closeForDestroy() throws IOException {
            close(false);
        }

        private void close(boolean completePendingCommand) throws IOException {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            activeStreams.remove(this);
            Throwable failure = null;
            try {
                super.close();
            } catch (Throwable throwable) {
                failure = throwable;
            } finally {
                if (completePendingCommand) {
                    try {
                        completePendingCommand();
                    } catch (Throwable throwable) {
                        failure = appendFailure(failure, throwable);
                    }
                }
                permit.release();
            }
            throwFailure(failure);
        }
    }

    private boolean isFileExistLocked(String path) throws IOException {
        if (isDirectoryExistLocked(path)) {
            return false;
        }
        FTPFile file = singleFile(path);
        return file != null && file.isFile();
    }

    private FTPFile singleFile(String path) throws IOException {
        FTPFile[] files = ftpClient.listFiles(encodeISO(path));
        return files != null && files.length == 1 ? files[0] : null;
    }

    @Override
    public boolean isFileExist(String path) throws IOException {
        TransferableReentrantLock.Permit permit = acquireIoLock();
        try {
            ensureReady();
            return isFileExistLocked(path);
        } finally {
            permit.release();
        }
    }

    @Override
    public boolean move(String sourcePath, String destPath) throws IOException {
        TransferableReentrantLock.Permit permit = acquireIoLock();
        try {
            ensureReady();
            return ftpClient.rename(encodeISO(sourcePath), encodeISO(destPath));
        } finally {
            permit.release();
        }
    }

    @Override
    public boolean delete(String path) throws IOException {
        TransferableReentrantLock.Permit permit = acquireIoLock();
        try {
            ensureReady();
            return ftpClient.deleteFile(encodeISO(path));
        } finally {
            permit.release();
        }
    }

    @Override
    public TapFile saveFile(String path, InputStream inputStream, boolean canReplace) throws IOException {
        TransferableReentrantLock.Permit permit = acquireIoLock();
        try {
            ensureReady();
            if (isFileExistLocked(path) && !canReplace) {
                return getFileLocked(path);
            }
            if (!ftpClient.storeFile(encodeISO(path), inputStream)) {
                throw new IOException("save ftp file failed: " + ftpClient.getReplyString());
            }
            return getFileLocked(path);
        } finally {
            permit.release();
        }
    }

    private TapFile getFileLocked(String path) throws IOException {
        FTPFile file = singleFile(path);
        return file == null ? null : toTapFile(file, path);
    }

    @Override
    public OutputStream openFileOutputStream(String path, boolean append) throws IOException {
        TransferableReentrantLock.Permit permit = acquireIoLock();
        boolean ownershipTransferred = false;
        try {
            ensureReady();
            OutputStream outputStream = append
                    ? ftpClient.appendFileStream(encodeISO(path))
                    : ftpClient.storeFileStream(encodeISO(path));
            if (outputStream == null) {
                throw new IOException("open ftp output stream failed: " + ftpClient.getReplyString());
            }
            ManagedOutputStream managed = new ManagedOutputStream(outputStream, permit);
            if (!managed.isRegistered()) {
                managed.close();
                throw new IOException("ftp storage is being destroyed");
            }
            ownershipTransferred = true;
            return managed;
        } finally {
            if (!ownershipTransferred) {
                permit.release();
            }
        }
    }

    private class ManagedOutputStream extends FilterOutputStream implements ForceCloseable {
        private final TransferableReentrantLock.Permit permit;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final boolean registered;

        private ManagedOutputStream(OutputStream outputStream, TransferableReentrantLock.Permit permit) {
            super(outputStream);
            this.permit = permit;
            this.registered = registerActiveStream(this);
        }

        private boolean isRegistered() {
            return registered;
        }

        @Override
        public void close() throws IOException {
            close(true);
        }

        @Override
        public void closeForDestroy() throws IOException {
            close(false);
        }

        private void close(boolean completePendingCommand) throws IOException {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            activeStreams.remove(this);
            Throwable failure = null;
            try {
                super.close();
            } catch (Throwable throwable) {
                failure = throwable;
            } finally {
                if (completePendingCommand) {
                    try {
                        completePendingCommand();
                    } catch (Throwable throwable) {
                        failure = appendFailure(failure, throwable);
                    }
                }
                permit.release();
            }
            throwFailure(failure);
        }
    }

    private void completePendingCommand() throws IOException {
        if (!ftpClient.completePendingCommand()) {
            throw new IOException("ftp data transfer did not complete: " + ftpClient.getReplyString());
        }
    }

    @Override
    public void getFilesInDirectory(String directoryPath,
                                    Collection<String> includeRegs,
                                    Collection<String> excludeRegs,
                                    boolean recursive,
                                    int batchSize,
                                    Consumer<List<TapFile>> consumer) throws IOException {
        TransferableReentrantLock.Permit permit = acquireIoLock();
        try {
            ensureReady();
            int effectiveBatchSize = batchSize > 0 ? batchSize : 1;
            AtomicReference<List<TapFile>> list = new AtomicReference<>(new ArrayList<>());
            getFiles(directoryPath, includeRegs, excludeRegs, recursive, effectiveBatchSize, consumer, list);
            if (!list.get().isEmpty()) {
                consumer.accept(list.get());
            }
        } finally {
            permit.release();
        }
    }

    private void getFiles(String directoryPath,
                          Collection<String> includeRegs,
                          Collection<String> excludeRegs,
                          boolean recursive,
                          int batchSize,
                          Consumer<List<TapFile>> consumer,
                          AtomicReference<List<TapFile>> list) throws IOException {
        FTPFile[] files = ftpClient.listFiles(encodeISO(directoryPath));
        if (files == null) {
            return;
        }
        for (FTPFile ftpFile : files) {
            if (ftpFile.isFile()) {
                if (FileMatchKit.matchRegs(ftpFile.getName(), includeRegs, excludeRegs)) {
                    list.get().add(toTapFile(ftpFile, getAbsolutePath(directoryPath, ftpFile.getName())));
                    if (list.get().size() >= batchSize) {
                        consumer.accept(list.get());
                        list.set(new ArrayList<>());
                    }
                }
            } else if (ftpFile.isDirectory() && recursive) {
                getFiles(getAbsolutePath(directoryPath, ftpFile.getName()), includeRegs, excludeRegs, true, batchSize, consumer, list);
            }
        }
    }

    @Override
    public boolean isDirectoryExist(String path) throws IOException {
        TransferableReentrantLock.Permit permit = acquireIoLock();
        try {
            ensureReady();
            return isDirectoryExistLocked(path);
        } finally {
            permit.release();
        }
    }

    private boolean isDirectoryExistLocked(String path) throws IOException {
        String currentDirectory = ftpClient.printWorkingDirectory();
        boolean exists = ftpClient.changeWorkingDirectory(encodeISO(path));
        if (currentDirectory != null) {
            ftpClient.changeWorkingDirectory(currentDirectory);
        }
        return exists;
    }

    @Override
    public String getConnectInfo() {
        FtpConfig config = ftpConfig;
        if (config == null) {
            return "ftp://";
        }
        return "ftp://" + config.getFtpHost() + (config.getFtpPort() == 21 ? "" : (":" + config.getFtpPort())) + "/";
    }

    private void ensureReady() throws IOException {
        if (ftpClient == null || ftpConfig == null) {
            throw new IOException("ftp storage is not initialized");
        }
    }

    private String encodeISO(String path) {
        try {
            String encoding = ftpConfig.getEncoding();
            return new String(path.getBytes(encoding), StandardCharsets.ISO_8859_1);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Unsupported FTP path encoding", e);
        }
    }

    private String getAbsolutePath(String parentPath, String fileName) {
        if (parentPath.endsWith("/")) {
            return parentPath + fileName;
        }
        return parentPath + "/" + fileName;
    }

    private Throwable appendFailure(Throwable failure, Throwable additionalFailure) {
        if (failure == null) {
            return additionalFailure;
        }
        if (failure != additionalFailure) {
            failure.addSuppressed(additionalFailure);
        }
        return failure;
    }

    private void throwFailure(Throwable failure) throws IOException {
        if (failure == null) {
            return;
        }
        if (failure instanceof IOException) {
            throw (IOException) failure;
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new IOException("FTP file operation failed", failure);
    }
}
