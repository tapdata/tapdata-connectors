package io.tapdata.storage.sftp;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.storage.kit.EmptyKit;
import io.tapdata.storage.kit.FileMatchKit;

import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class SftpFileStorage implements TapFileStorage {

    private final ReentrantLock operationLock = new ReentrantLock(true);
    private SftpConfig sftpConfig;
    private Session session;
    private ChannelSftp channel;

    @Override
    public void init(Map<String, Object> params) throws JSchException, SftpException {
        operationLock.lock();
        try {
            destroyUnlocked();
            sftpConfig = new SftpConfig().load(params);
            sftpConfig.validate();

            JSch jsch = new JSch();
            if (EmptyKit.isNotBlank(sftpConfig.getSftpKnownHosts())) {
                jsch.setKnownHosts(sftpConfig.getSftpKnownHosts());
            }
            Session newSession = jsch.getSession(sftpConfig.getSftpUsername(),
                    sftpConfig.getSftpHost(), sftpConfig.getSftpPort());
            if (EmptyKit.isNotBlank(sftpConfig.getSftpPassword())) {
                newSession.setPassword(sftpConfig.getSftpPassword());
            }
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", sftpConfig.getSftpStrictHostKeyChecking());
            newSession.setConfig(config);
            newSession.setTimeout(sftpConfig.getSftpConnectionTimeoutMillis());
            ChannelSftp newChannel = null;
            try {
                newSession.connect(sftpConfig.getSftpConnectionTimeoutMillis());
                newChannel = (ChannelSftp) newSession.openChannel("sftp");
                newChannel.connect(sftpConfig.getSftpConnectionTimeoutMillis());
                newChannel.setFilenameEncoding(sftpConfig.getEncoding());
                session = newSession;
                channel = newChannel;
            } catch (JSchException | SftpException e) {
                if (newChannel != null) newChannel.disconnect();
                newSession.disconnect();
                throw e;
            }
        } finally {
            operationLock.unlock();
        }
    }

    @Override
    public void destroy() {
        operationLock.lock();
        try {
            destroyUnlocked();
        } finally {
            operationLock.unlock();
        }
    }

    private void destroyUnlocked() {
        if (channel != null) {
            channel.disconnect();
            channel = null;
        }
        if (session != null) {
            session.disconnect();
            session = null;
        }
    }

    @Override
    public TapFile getFile(String path) throws SftpException {
        operationLock.lock();
        try {
            ensureConnected();
            SftpATTRS attrs;
            try {
                attrs = channel.stat(path);
            } catch (SftpException e) {
                if (isNotFound(e)) return null;
                throw e;
            }
            return toTapFile(attrs, path);
        } finally {
            operationLock.unlock();
        }
    }

    @Override
    public void readFile(String path, Consumer<InputStream> consumer) throws Exception {
        InputStream inputStream = readFile(path);
        if (inputStream == null) return;
        try (InputStream managed = inputStream) {
            consumer.accept(managed);
        }
    }

    @Override
    public InputStream readFile(String path) throws Exception {
        operationLock.lock();
        try {
            ensureConnected();
            if (!isFileExistUnlocked(path)) {
                operationLock.unlock();
                return null;
            }
            try {
                return new UnlockingInputStream(channel.get(path), operationLock);
            } catch (SftpException e) {
                operationLock.unlock();
                throw e;
            }
        } catch (Exception | Error e) {
            if (operationLock.isHeldByCurrentThread()) operationLock.unlock();
            throw e;
        }
    }

    @Override
    public boolean isFileExist(String path) throws SftpException {
        operationLock.lock();
        try {
            ensureConnected();
            return isFileExistUnlocked(path);
        } finally {
            operationLock.unlock();
        }
    }

    private boolean isFileExistUnlocked(String path) throws SftpException {
        try {
            return !channel.stat(path).isDir();
        } catch (SftpException e) {
            if (isNotFound(e)) return false;
            throw e;
        }
    }

    @Override
    public boolean move(String sourcePath, String destPath) {
        throw new UnsupportedOperationException("SFTP move is not supported");
    }

    @Override
    public boolean delete(String path) throws SftpException {
        operationLock.lock();
        try {
            ensureConnected();
            SftpATTRS attrs;
            try {
                attrs = channel.stat(path);
            } catch (SftpException e) {
                if (isNotFound(e)) return false;
                throw e;
            }
            if (attrs.isDir()) {
                deleteDirUnlocked(path);
            } else {
                channel.rm(path);
            }
            return true;
        } catch (SftpException e) {
            if (isNotFound(e)) return false;
            throw e;
        } finally {
            operationLock.unlock();
        }
    }

    private void deleteDirUnlocked(String path) throws SftpException {
        for (Object item : channel.ls(path)) {
            ChannelSftp.LsEntry lsEntry = (ChannelSftp.LsEntry) item;
            String fileName = lsEntry.getFilename();
            if (".".equals(fileName) || "..".equals(fileName)) continue;
            String childPath = getAbsolutePath(path, fileName);
            if (lsEntry.getAttrs().isDir()) {
                deleteDirUnlocked(childPath);
            } else {
                channel.rm(childPath);
            }
        }
        channel.rmdir(path);
    }

    @Override
    public TapFile saveFile(String path, InputStream inputStream, boolean canReplace) throws SftpException {
        operationLock.lock();
        try {
            ensureConnected();
            if (isFileExistUnlocked(path) && !canReplace) return getFileUnlocked(path);
            channel.put(inputStream, path, ChannelSftp.OVERWRITE);
            return getFileUnlocked(path);
        } finally {
            operationLock.unlock();
        }
    }

    @Override
    public OutputStream openFileOutputStream(String path, boolean append) throws Exception {
        operationLock.lock();
        try {
            ensureConnected();
            return new UnlockingOutputStream(channel.put(path,
                    append ? ChannelSftp.APPEND : ChannelSftp.OVERWRITE), operationLock);
        } catch (SftpException e) {
            operationLock.unlock();
            throw e;
        } catch (RuntimeException | Error e) {
            operationLock.unlock();
            throw e;
        }
    }

    @Override
    public void getFilesInDirectory(String directoryPath,
                                    Collection<String> includeRegs,
                                    Collection<String> excludeRegs,
                                    boolean recursive,
                                    int batchSize,
                                    Consumer<List<TapFile>> consumer) throws SftpException {
        operationLock.lock();
        try {
            ensureConnected();
            if (batchSize <= 0) throw new IllegalArgumentException("batchSize must be positive");
            if (!isDirectoryExistUnlocked(directoryPath)) return;
            List<TapFile> batch = new ArrayList<>();
            getFiles(directoryPath, includeRegs, excludeRegs, recursive, batchSize, consumer, batch);
            if (!batch.isEmpty()) consumer.accept(batch);
        } finally {
            operationLock.unlock();
        }
    }

    private void getFiles(String directoryPath,
                          Collection<String> includeRegs,
                          Collection<String> excludeRegs,
                          boolean recursive,
                          int batchSize,
                          Consumer<List<TapFile>> consumer,
                          List<TapFile> batch) throws SftpException {
        for (Object item : channel.ls(directoryPath)) {
            ChannelSftp.LsEntry lsEntry = (ChannelSftp.LsEntry) item;
            String fileName = lsEntry.getFilename();
            if (".".equals(fileName) || "..".equals(fileName)) continue;
            if (!lsEntry.getAttrs().isDir()) {
                if (FileMatchKit.matchRegs(fileName, includeRegs, excludeRegs)) {
                    batch.add(toTapFile(lsEntry.getAttrs(), getAbsolutePath(directoryPath, fileName)));
                    if (batch.size() >= batchSize) {
                        consumer.accept(new ArrayList<>(batch));
                        batch.clear();
                    }
                }
            } else if (recursive) {
                getFiles(getAbsolutePath(directoryPath, fileName), includeRegs, excludeRegs,
                        true, batchSize, consumer, batch);
            }
        }
    }

    private TapFile getFileUnlocked(String path) throws SftpException {
        try {
            return toTapFile(channel.stat(path), path);
        } catch (SftpException e) {
            if (isNotFound(e)) return null;
            throw e;
        }
    }

    private TapFile toTapFile(SftpATTRS attrs, String path) {
        return new TapFile().type(attrs.isDir() ? TapFile.TYPE_DIRECTORY : TapFile.TYPE_FILE)
                .name(path.substring(path.lastIndexOf("/") + 1)).path(path)
                .length(attrs.getSize()).lastModified(attrs.getMTime() * 1000L);
    }

    @Override
    public boolean isDirectoryExist(String path) throws SftpException {
        operationLock.lock();
        try {
            ensureConnected();
            return isDirectoryExistUnlocked(path);
        } finally {
            operationLock.unlock();
        }
    }

    private boolean isDirectoryExistUnlocked(String path) throws SftpException {
        try {
            return channel.stat(path).isDir();
        } catch (SftpException e) {
            if (isNotFound(e)) return false;
            throw e;
        }
    }

    @Override
    public String getConnectInfo() {
        if (sftpConfig == null) return "sftp://";
        return "sftp://" + sftpConfig.getSftpHost()
                + (sftpConfig.getSftpPort() == 22 ? "" : (":" + sftpConfig.getSftpPort())) + "/";
    }

    private void ensureConnected() throws SftpException {
        if (session == null || !session.isConnected() || channel == null || !channel.isConnected()) {
            throw new SftpException(ChannelSftp.SSH_FX_CONNECTION_LOST, "SFTP session/channel is down");
        }
    }

    private boolean isNotFound(SftpException exception) {
        return exception.id == ChannelSftp.SSH_FX_NO_SUCH_FILE;
    }

    private String getAbsolutePath(String parentPath, String fileName) {
        return parentPath.endsWith("/") ? parentPath + fileName : parentPath + "/" + fileName;
    }

    private static final class UnlockingInputStream extends FilterInputStream {
        private final ReentrantLock lock;
        private final AtomicBoolean closed = new AtomicBoolean();

        private UnlockingInputStream(InputStream delegate, ReentrantLock lock) {
            super(delegate);
            this.lock = lock;
        }

        @Override
        public void close() throws IOException {
            if (!closed.compareAndSet(false, true)) return;
            try {
                super.close();
            } finally {
                lock.unlock();
            }
        }
    }

    private static final class UnlockingOutputStream extends FilterOutputStream {
        private final ReentrantLock lock;
        private final AtomicBoolean closed = new AtomicBoolean();

        private UnlockingOutputStream(OutputStream delegate, ReentrantLock lock) {
            super(delegate);
            this.lock = lock;
        }

        @Override
        public void close() throws IOException {
            if (!closed.compareAndSet(false, true)) return;
            try {
                super.close();
            } finally {
                lock.unlock();
            }
        }
    }
}
