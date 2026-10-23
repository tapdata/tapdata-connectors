package io.tapdata.common;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.kit.EmptyKit;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public abstract class FileSchema {

    private static final String TAG = FileSchema.class.getSimpleName();
    private static final long WORKER_SHUTDOWN_TIMEOUT_SECONDS = 10;

    protected final FileConfig fileConfig;
    protected final TapFileStorage storage;

    public FileSchema(FileConfig fileConfig, TapFileStorage storage) {
        this.fileConfig = fileConfig;
        this.storage = storage;
    }

    public Map<String, Object> sampleEveryFileData(ConcurrentMap<String, TapFile> fileMap) {
        Map<String, Object> sampleResult = new LinkedHashMap<>();
        ExecutorService executorService = Executors.newFixedThreadPool(5);
        CountDownLatch countDownLatch = new CountDownLatch(5);
        List<Exception> exceptionList = new CopyOnWriteArrayList<>();
        for (int i = 0; i < 5; i++) {
            executorService.submit(() -> {
                try {
                    TapFile file;
                    while (!Thread.currentThread().isInterrupted() && (file = getOutFile(fileMap)) != null) {
                        try {
                            sampleOneFile(sampleResult, file);
                        } catch (Exception e) {
                            exceptionList.add(e);
                            if (e instanceof InterruptedException) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                    }
                } finally {
                    countDownLatch.countDown();
                }
            });
        }
        boolean samplingInterrupted = false;
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            samplingInterrupted = true;
            throw new RuntimeException("Interrupted while sampling file data", e);
        } finally {
            boolean shutdownInterrupted = shutdownExecutorAndAwaitTermination(executorService, samplingInterrupted);
            if (samplingInterrupted || shutdownInterrupted) {
                Thread.currentThread().interrupt();
            }
        }
        if (EmptyKit.isNotEmpty(exceptionList)) {
            throw new RuntimeException("sample every file error", exceptionList.get(0));
        }
        return sampleResult;
    }

    private boolean shutdownExecutorAndAwaitTermination(ExecutorService executorService, boolean closeStorage) {
        executorService.shutdownNow();
        if (closeStorage) {
            try {
                // Closing active streams gives blocking storage reads a chance to finish before the caller's cleanup.
                storage.destroy();
            } catch (Throwable e) {
                TapLogger.warn(TAG, "Failed to close storage after sampling was interrupted", e);
            }
        }
        boolean interrupted = false;
        try {
            if (!executorService.awaitTermination(WORKER_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                TapLogger.warn(TAG, "Sampling workers did not terminate within "
                        + WORKER_SHUTDOWN_TIMEOUT_SECONDS + " seconds");
            }
        } catch (InterruptedException e) {
            interrupted = true;
            TapLogger.warn(TAG, "Interrupted while waiting for sampling workers to terminate", e);
        }
        return interrupted;
    }

    public Map<String, Object> sampleFixedFileData(Map<String, TapFile> csvFileMap) throws Exception {
        throw new UnsupportedOperationException();
    }

    protected abstract void sampleOneFile(Map<String, Object> sampleResult, TapFile tapFile) throws Exception;

    protected synchronized TapFile getOutFile(ConcurrentMap<String, TapFile> fileMap) {
        if (EmptyKit.isNotEmpty(fileMap)) {
            String path = fileMap.keySet().stream().findFirst().orElseGet(String::new);
            TapFile tapFile = fileMap.get(path);
            fileMap.remove(path);
            return tapFile;
        }
        return null;
    }

    protected void putIntoMap(String[] headers, String[] data, Map<String, Object> sampleResult) {
        if (EmptyKit.isNull(data)) {
            for (String header : headers) {
                putValidIntoMap(sampleResult, header, "");
            }
        } else {
            for (int i = 0; i < headers.length && i < data.length; i++) {
                putValidIntoMap(sampleResult, headers[i], data[i]);
            }
            for (int i = 0; i < headers.length - data.length; i++) {
                putValidIntoMap(sampleResult, headers[i + data.length], "");
            }
        }
    }

    protected synchronized void putValidIntoMap(Map<String, Object> map, String key, Object value) {
        if (!map.containsKey(key) || EmptyKit.isBlank((String) map.get(key))) {
            map.put(key, value);
        }
    }
}
