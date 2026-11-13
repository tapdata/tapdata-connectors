package io.tapdata.connector.paimon.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Local scratch validation and best-effort evidence, without dumping connection properties. */
public final class PaimonLocalDirectories {
    private PaimonLocalDirectories() { }

    /** Observations only: free space is neither reserved nor proof of a particular failure cause. */
    public static Map<String, Object> describe(Path path) {
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("path", path.toString());
        try {
            state.put("exists", Files.exists(path));
            state.put("directory", Files.isDirectory(path));
            Path existing = path.toAbsolutePath();
            while (existing != null && !Files.exists(existing)) {
                existing = existing.getParent();
            }
            if (existing != null) {
                state.put("observedAncestor", existing.toRealPath().toString());
                FileStore store = Files.getFileStore(existing);
                state.put("fileStore", store.name());
                state.put("fileStoreType", store.type());
                state.put("readOnly", store.isReadOnly());
                state.put("usableBytes", store.getUsableSpace());
                state.put("totalBytes", store.getTotalSpace());
            }
        } catch (IOException | RuntimeException failure) {
            state.put("observationFailure", failure.toString());
        }
        return state;
    }

    public static List<Map<String, Object>> describeRoots(java.util.Collection<String> roots) {
        List<Map<String, Object>> states = new ArrayList<>();
        for (String root : roots) {
            if (states.size() >= 8) { break; }
            try {
                states.add(describe(Paths.get(root)));
            } catch (RuntimeException invalid) {
                Map<String, Object> state = new LinkedHashMap<>();
                state.put("observationFailure", invalid.toString());
                states.add(state);
            }
        }
        return states;
    }

    public static String verifyS3ABufferDirs(String configured) {
        List<Path> roots = new ArrayList<>();
        for (String raw : configured.split(",", -1)) {
            int index = roots.size() + 1;
            String dir = raw.trim();
            if (dir.isEmpty()) {
                throw new IllegalArgumentException("fs.s3a.buffer.dir entry " + index
                        + " must be an absolute local directory, not an empty entry");
            }
            Path candidate;
            try {
                candidate = Paths.get(dir);
            } catch (RuntimeException invalid) {
                throw new IllegalArgumentException("fs.s3a.buffer.dir entry " + index
                        + " is not a usable local directory path: " + dir);
            }
            if (!candidate.isAbsolute()) {
                throw new IllegalArgumentException("fs.s3a.buffer.dir entry " + index
                        + " must be an absolute local directory: " + dir);
            }
            roots.add(candidate);
        }
        List<String> verified = new ArrayList<>(roots.size());
        List<Path> created = new ArrayList<>();
        try {
            for (int i = 0; i < roots.size(); i++) {
                Path path = roots.get(i);
                File dir = path.toFile();
                if (dir.exists() && !dir.isDirectory()) {
                    throw new IllegalStateException("fs.s3a.buffer.dir entry " + (i + 1) + " (" + dir
                            + ") exists but is not a directory");
                }
                if (!dir.isDirectory()) {
                    try {
                        createDirectories(path, created);
                    } catch (IOException | RuntimeException failure) {
                        throw new IllegalStateException("fs.s3a.buffer.dir entry " + (i + 1) + " (" + dir
                                + ") does not exist and cannot be created; " + describe(path), failure);
                    }
                }
                Path probe = path.resolve(".tapdata-s3a-buffer-probe-"
                        + UUID.randomUUID().toString().replace("-", ""));
                try {
                    Files.write(probe, new byte[] {0}, java.nio.file.StandardOpenOption.CREATE_NEW,
                            java.nio.file.StandardOpenOption.WRITE);
                } catch (IOException | RuntimeException failure) {
                    throw new IllegalStateException("fs.s3a.buffer.dir entry " + (i + 1) + " (" + dir
                            + ") is not writable by the engine process: " + failure + "; " + describe(path), failure);
                } finally {
                    try {
                        Files.deleteIfExists(probe);
                    } catch (IOException | RuntimeException ignore) {
                        // A leftover probe file is harmless; the writability answer is already known.
                        // RuntimeException 也必须吞掉：SecurityException 从这里抛出会顶掉待传播的失败原因。
                    }
                }
                verified.add(dir.toString());
            }
        } catch (RuntimeException failure) {
            // 只回收本次新建的目录；非空目录删除会抛异常并被忽略，不会误删其他进程的数据。
            for (int i = created.size() - 1; i >= 0; i--) {
                try {
                    Files.deleteIfExists(created.get(i));
                } catch (IOException | RuntimeException ignore) {
                    // Directory is in use: leave it alone.
                    // RuntimeException（如 SecurityException）不能在此逃逸，否则回滚会顶掉原始失败原因。
                }
            }
            throw failure;
        }
        return String.join(",", verified);
    }

    private static void createDirectories(Path path, List<Path> created) throws IOException {
        if (Files.isDirectory(path)) { return; }
        Path parent = path.getParent();
        if (parent != null) { createDirectories(parent, created); }
        try {
            Files.createDirectory(path);
            created.add(path);
        } catch (java.nio.file.FileAlreadyExistsException raced) {
            if (!Files.isDirectory(path)) { throw raced; }
        }
    }

}
