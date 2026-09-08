package io.tapdata.storage.ftp;

import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.file.operation.FileStorageCapability;
import io.tapdata.storage.kit.FileMatchKit;
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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

public class FtpFileStorage implements TapFileStorage {

    private final Semaphore ioLock = new Semaphore(1, true);
    private FtpConfig ftpConfig;
    private FTPClient ftpClient;

    @Override
    public void init(Map<String, Object> params) throws IOException {
        ioLock.acquireUninterruptibly();
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
            ioLock.release();
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
            throw new UnsupportedOperationException("FTPS is not supported by ftp-file yet");
        }
    }

    @Override
    public void destroy() throws IOException {
        ioLock.acquireUninterruptibly();
        try {
            IOException failure = closeClient(ftpClient);
            ftpClient = null;
            ftpConfig = null;
            if (failure != null) {
                throw failure;
            }
        } finally {
            ioLock.release();
        }
    }

    private IOException closeClient(FTPClient client) {
        if (client == null) {
            return null;
        }
        IOException failure = null;
        try {
            if (client.isConnected()) {
                client.logout();
            }
        } catch (IOException e) {
            failure = e;
        } finally {
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
        }
        return failure;
    }

    @Override
    public TapFile getFile(String path) throws IOException {
        ioLock.acquireUninterruptibly();
        try {
            ensureReady();
            FTPFile file = singleFile(path);
            if (file != null) {
                return toTapFile(file, path);
            }
            if (isDirectoryExistLocked(path)) {
                return toDirectory(path);
            }
            return null;
        } finally {
            ioLock.release();
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
        ioLock.acquireUninterruptibly();
        try {
            ensureReady();
            if (!isFileExistLocked(path)) {
                return;
            }
            InputStream inputStream = ftpClient.retrieveFileStream(encodeISO(path));
            if (inputStream == null) {
                throw new IOException("open ftp input stream failed: " + ftpClient.getReplyString());
            }
            Throwable failure = null;
            try {
                consumer.accept(inputStream);
            } catch (Throwable throwable) {
                failure = throwable;
            } finally {
                try {
                    inputStream.close();
                } catch (Throwable throwable) {
                    failure = appendFailure(failure, throwable);
                }
                try {
                    completePendingCommand();
                } catch (Throwable throwable) {
                    failure = appendFailure(failure, throwable);
                }
            }
            throwFailure(failure);
        } finally {
            ioLock.release();
        }
    }

    @Override
    public InputStream readFile(String path) throws IOException {
        ioLock.acquireUninterruptibly();
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
            ownershipTransferred = true;
            return new ManagedInputStream(inputStream);
        } finally {
            if (!ownershipTransferred) {
                ioLock.release();
            }
        }
    }

    private class ManagedInputStream extends FilterInputStream {
        private boolean closed;

        private ManagedInputStream(InputStream inputStream) {
            super(inputStream);
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            Throwable failure = null;
            try {
                super.close();
            } catch (Throwable throwable) {
                failure = throwable;
            } finally {
                try {
                    completePendingCommand();
                } catch (Throwable throwable) {
                    failure = appendFailure(failure, throwable);
                } finally {
                    ioLock.release();
                }
            }
            throwFailure(failure);
        }
    }

    private boolean isFileExistLocked(String path) throws IOException {
        FTPFile file = singleFile(path);
        return file != null && file.isFile();
    }

    private FTPFile singleFile(String path) throws IOException {
        FTPFile[] files = ftpClient.listFiles(encodeISO(path));
        return files != null && files.length == 1 ? files[0] : null;
    }

    @Override
    public boolean isFileExist(String path) throws IOException {
        ioLock.acquireUninterruptibly();
        try {
            ensureReady();
            return isFileExistLocked(path);
        } finally {
            ioLock.release();
        }
    }

    @Override
    public boolean move(String sourcePath, String destPath) throws IOException {
        ioLock.acquireUninterruptibly();
        try {
            ensureReady();
            return ftpClient.rename(encodeISO(sourcePath), encodeISO(destPath));
        } finally {
            ioLock.release();
        }
    }

    @Override
    public boolean delete(String path) throws IOException {
        ioLock.acquireUninterruptibly();
        try {
            ensureReady();
            return ftpClient.deleteFile(encodeISO(path));
        } finally {
            ioLock.release();
        }
    }

    @Override
    public TapFile saveFile(String path, InputStream inputStream, boolean canReplace) throws IOException {
        ioLock.acquireUninterruptibly();
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
            ioLock.release();
        }
    }

    private TapFile getFileLocked(String path) throws IOException {
        FTPFile file = singleFile(path);
        return file == null ? null : toTapFile(file, path);
    }

    @Override
    public OutputStream openFileOutputStream(String path, boolean append) throws IOException {
        ioLock.acquireUninterruptibly();
        boolean ownershipTransferred = false;
        try {
            ensureReady();
            OutputStream outputStream = append
                    ? ftpClient.appendFileStream(encodeISO(path))
                    : ftpClient.storeFileStream(encodeISO(path));
            if (outputStream == null) {
                throw new IOException("open ftp output stream failed: " + ftpClient.getReplyString());
            }
            ownershipTransferred = true;
            return new ManagedOutputStream(outputStream);
        } finally {
            if (!ownershipTransferred) {
                ioLock.release();
            }
        }
    }

    private class ManagedOutputStream extends FilterOutputStream {
        private boolean closed;

        private ManagedOutputStream(OutputStream outputStream) {
            super(outputStream);
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            Throwable failure = null;
            try {
                super.close();
            } catch (Throwable throwable) {
                failure = throwable;
            } finally {
                try {
                    completePendingCommand();
                } catch (Throwable throwable) {
                    failure = appendFailure(failure, throwable);
                } finally {
                    ioLock.release();
                }
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
        ioLock.acquireUninterruptibly();
        try {
            ensureReady();
            int effectiveBatchSize = batchSize > 0 ? batchSize : 1;
            AtomicReference<List<TapFile>> list = new AtomicReference<>(new ArrayList<>());
            getFiles(directoryPath, includeRegs, excludeRegs, recursive, effectiveBatchSize, consumer, list);
            if (!list.get().isEmpty()) {
                consumer.accept(list.get());
            }
        } finally {
            ioLock.release();
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
        ioLock.acquireUninterruptibly();
        try {
            ensureReady();
            return isDirectoryExistLocked(path);
        } finally {
            ioLock.release();
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
    public EnumSet<FileStorageCapability> capabilities() {
        return EnumSet.of(FileStorageCapability.ATOMIC_RENAME, FileStorageCapability.APPEND);
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
