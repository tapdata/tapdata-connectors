package io.tapdata.oceanbase.runtime;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public final class OceanbaseCdcRuntime {

    public static final String VERSION = "4.4.2.1";
    public static final String PLATFORM = "linux-x86_64";
    public static final String CACHE_PROPERTY = "tapdata.native.cache.dir";
    public static final String RESOURCE = "/native/obcdc/" + VERSION + "/" + PLATFORM + "/runtime.zip";

    private static final String COMPLETE_FILE = ".complete";
    private static final Map<String, String> FILE_HASHES = new LinkedHashMap<>();
    private static final Map<String, Long> FILE_SIZES = new LinkedHashMap<>();

    static {
        FILE_HASHES.put("lib64/libobcdc.so.4.4.2.1", "dac5929cefb923cf5b40c2a0ea1c2d93bd067ad6245d6bfc7d369b82f264d767");
        FILE_HASHES.put("etc/libobcdc.conf", "b92ae713b45bf60687edf6df28c588b5256c5b5223670a0022c67402f7596007");
        FILE_HASHES.put("etc/obcdc_compatiable_ob_info.yaml", "9ae662a379c78907fe359c7233c20c7d574a2d8655a92005dddb570e6c6e2966");
        FILE_HASHES.put("etc/timezone_info.conf", "8902e04a06925d9bfd22919b9e346689882eddae2a6225cd2270ba433c70fce3");
        FILE_SIZES.put("lib64/libobcdc.so.4.4.2.1", 1031381816L);
        FILE_SIZES.put("etc/libobcdc.conf", 5000L);
        FILE_SIZES.put("etc/obcdc_compatiable_ob_info.yaml", 797L);
        FILE_SIZES.put("etc/timezone_info.conf", 11940260L);
    }

    private OceanbaseCdcRuntime() {
    }

    public static File prepare(Class<?> resourceOwner) throws IOException {
        validatePlatform();
        ClassLoader classLoader = resourceOwner.getClassLoader();
        if (classLoader == null) {
            classLoader = ClassLoader.getSystemClassLoader();
        }
        InputStream input = classLoader.getResourceAsStream(RESOURCE.substring(1));
        if (input == null) {
            input = resourceOwner.getResourceAsStream(RESOURCE);
        }
        if (input == null) {
            throw new IOException("OceanBase CDC runtime " + RESOURCE + " not found in connector package");
        }
        try (InputStream runtimeZip = input) {
            return prepare(runtimeZip, cacheRoot()).toFile();
        }
    }

    static Path prepare(InputStream runtimeZip, Path cacheRoot) throws IOException {
        Path platformRoot = cacheRoot.resolve("oceanbase-cdc").resolve(VERSION).resolve(PLATFORM);
        Path runtimeHome = platformRoot.resolve("runtime");
        Files.createDirectories(platformRoot);
        Path lockPath = platformRoot.resolve("install.lock");
        try (FileChannel channel = FileChannel.open(lockPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock ignored = channel.lock()) {
            if (isComplete(runtimeHome)) {
                return runtimeHome;
            }
            Path staging = platformRoot.resolve("runtime.tmp-" + UUID.randomUUID());
            deleteRecursively(staging);
            Files.createDirectories(staging);
            try {
                extract(runtimeZip, staging);
                createLibraryLinks(staging);
                verify(staging);
                Files.write(staging.resolve(COMPLETE_FILE), completionMarker().getBytes(StandardCharsets.UTF_8));
                deleteRecursively(runtimeHome);
                move(staging, runtimeHome);
            } catch (Exception throwable) {
                deleteRecursively(staging);
                if (throwable instanceof IOException) {
                    throw (IOException) throwable;
                }
                throw new IOException("Failed to install OceanBase CDC runtime", throwable);
            }
            return runtimeHome;
        }
    }

    private static Path cacheRoot() {
        String configured = System.getProperty(CACHE_PROPERTY);
        if (configured != null && !configured.trim().isEmpty()) {
            return new File(configured.trim()).toPath();
        }
        String userHome = System.getProperty("user.home");
        if (userHome != null && !userHome.trim().isEmpty()) {
            return new File(userHome, ".tapdata/native").toPath();
        }
        return new File(System.getProperty("java.io.tmpdir"), "tapdata-native").toPath();
    }

    private static void validatePlatform() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        if (!os.contains("linux") || !("amd64".equals(arch) || "x86_64".equals(arch))) {
            throw new IllegalStateException("OceanBase CDC runtime " + VERSION
                    + " supports Linux x86_64 only, current platform is " + os + " " + arch);
        }
        if (new File("/etc/alpine-release").isFile()) {
            throw new IllegalStateException("OceanBase CDC runtime " + VERSION
                    + " requires glibc and does not support Alpine Linux/musl");
        }
    }

    private static void extract(InputStream input, Path target) throws IOException {
        try (ZipInputStream zip = new ZipInputStream(new BufferedInputStream(input))) {
            ZipEntry entry;
            byte[] buffer = new byte[1024 * 1024];
            while ((entry = zip.getNextEntry()) != null) {
                Path output = target.resolve(entry.getName()).normalize();
                if (!output.startsWith(target)) {
                    throw new IOException("Invalid runtime archive entry: " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(output);
                } else {
                    Files.createDirectories(output.getParent());
                    try (OutputStream out = new BufferedOutputStream(new FileOutputStream(output.toFile()))) {
                        int read;
                        while ((read = zip.read(buffer)) != -1) {
                            out.write(buffer, 0, read);
                        }
                    }
                }
                zip.closeEntry();
            }
        }
    }

    private static void createLibraryLinks(Path runtimeHome) throws IOException {
        Path libDir = runtimeHome.resolve("lib64");
        Files.createDirectories(libDir);
        createOrReplaceLink(libDir.resolve("libobcdc.so.4"), "libobcdc.so.4.4.2.1");
        createOrReplaceLink(libDir.resolve("libobcdc.so"), "libobcdc.so.4");
    }

    private static void createOrReplaceLink(Path link, String target) throws IOException {
        Files.deleteIfExists(link);
        Files.createSymbolicLink(link, new File(target).toPath());
    }

    private static boolean isComplete(Path runtimeHome) {
        try {
            Path marker = runtimeHome.resolve(COMPLETE_FILE);
            return Files.isRegularFile(marker)
                    && completionMarker().equals(new String(Files.readAllBytes(marker), StandardCharsets.UTF_8))
                    && layoutMatches(runtimeHome);
        } catch (IOException e) {
            return false;
        }
    }

    private static void verify(Path runtimeHome) throws IOException {
        if (!hashesMatch(runtimeHome)) {
            throw new IOException("OceanBase CDC runtime checksum verification failed");
        }
    }

    private static boolean hashesMatch(Path runtimeHome) throws IOException {
        for (Map.Entry<String, String> entry : FILE_HASHES.entrySet()) {
            Path file = runtimeHome.resolve(entry.getKey());
            if (!Files.isRegularFile(file) || !entry.getValue().equals(sha256(file))) {
                return false;
            }
        }
        return Files.isSymbolicLink(runtimeHome.resolve("lib64/libobcdc.so.4"))
                && Files.isSymbolicLink(runtimeHome.resolve("lib64/libobcdc.so"));
    }

    private static boolean layoutMatches(Path runtimeHome) throws IOException {
        for (Map.Entry<String, Long> entry : FILE_SIZES.entrySet()) {
            Path file = runtimeHome.resolve(entry.getKey());
            if (!Files.isRegularFile(file) || Files.size(file) != entry.getValue()) {
                return false;
            }
        }
        return Files.isSymbolicLink(runtimeHome.resolve("lib64/libobcdc.so.4"))
                && Files.isSymbolicLink(runtimeHome.resolve("lib64/libobcdc.so"));
    }

    private static String completionMarker() {
        StringBuilder marker = new StringBuilder(VERSION).append('\n');
        for (Map.Entry<String, String> entry : FILE_HASHES.entrySet()) {
            marker.append(entry.getValue()).append("  ").append(entry.getKey()).append('\n');
        }
        return marker.toString();
    }

    private static String sha256(Path file) throws IOException {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        try (InputStream input = new BufferedInputStream(new FileInputStream(file.toFile()))) {
            byte[] buffer = new byte[1024 * 1024];
            int read;
            while ((read = input.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        StringBuilder value = new StringBuilder(64);
        for (byte item : digest.digest()) {
            value.append(String.format("%02x", item & 0xff));
        }
        return value.toString();
    }

    private static void move(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target);
        }
    }

    private static void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        File[] children = path.toFile().listFiles();
        if (children != null) {
            Arrays.sort(children);
            for (File child : children) {
                if (Files.isSymbolicLink(child.toPath())) {
                    Files.deleteIfExists(child.toPath());
                } else {
                    deleteRecursively(child.toPath());
                }
            }
        }
        Files.deleteIfExists(path);
    }
}
