package io.tapdata.connector.paimon.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Spill 平台语义测试使用的独立 JVM 探针。
 *
 * <p>只由测试按需启动，并由 {@link Handle#close()} 回收；不搜索或终止其他进程。每个模式只做一件事，
 * 结果以单行 {@code key=value} 输出，父进程按前缀读取。
 */
public final class SpillProbeProcess {

    private SpillProbeProcess() {
    }

    public static void main(String[] args) throws Exception {
        switch (args[0]) {
            case "try-lock":
                tryLock(Paths.get(args[1]));
                break;
            case "open-then-lock":
                openThenLock(Paths.get(args[1]));
                break;
            case "runtime-name":
                runtimeName(Paths.get(args[1]), args[2]);
                break;
            case "hold-lock":
                holdLock(Paths.get(args[1]));
                break;
            case "jvm-id":
                System.out.println("jvmId=" + PaimonJvmIdentity.current());
                break;
            case "jvm-id-hosts":
                jvmIdAcrossHostResolution(Paths.get(args[1]), args[2]);
                break;
            case "legacy-cleanup":
                // develop e3f68298 的现有清理器；grace=0 只替代“目录空闲超过宽限期”，不参与存活判定。
                int deleted = PaimonSpillDirCleaner.cleanupStaleSpillDirs(new String[] {args[1]}, 0L, null);
                System.out.println("deleted=" + deleted);
                break;
            default:
                throw new IllegalArgumentException("Unknown probe mode " + args[0]);
        }
        System.out.flush();
    }

    /** 外部进程对锁文件的一次非阻塞探测；不创建文件。 */
    private static void tryLock(Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            FileLock lock = channel.tryLock();
            System.out.println("acquired=" + (lock != null));
            if (lock != null) {
                lock.release();
            }
        }
    }

    /** 以独立进程持有锁文件的排他锁，直到收到 release：模拟仍存活的外部 owner。 */
    private static void holdLock(Path file) throws IOException {
        BufferedReader commands = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
             FileLock lock = channel.tryLock()) {
            if (lock == null) {
                throw new IllegalStateException("Lock is already held elsewhere");
            }
            System.out.println("locked");
            System.out.flush();
            if (!"release".equals(commands.readLine())) {
                throw new IllegalStateException("Probe did not receive release");
            }
        }
    }

    /** 先打开（不加锁）并等待父进程指令，再 tryLock：模拟在 owner 退役前已打开锁文件的迟到回收者。 */
    private static void openThenLock(Path file) throws IOException {
        BufferedReader commands = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            System.out.println("opened");
            System.out.flush();
            if (!"go".equals(commands.readLine())) {
                throw new IllegalStateException("Probe did not receive go");
            }
            FileLock lock = channel.tryLock();
            System.out.println("acquired=" + (lock != null)
                    + " exists=" + Files.exists(file, LinkOption.NOFOLLOW_LINKS));
            if (lock != null) {
                lock.release();
            }
        }
    }

    /**
     * 在同一 JVM 内两次读取 RuntimeMXBean.getName()，中间让本机主机名变得不可解析。
     * JDK 17 VMManagementImpl.getVmId 每次调用都经 InetAddress.getLocalHost 解析主机名，失败时退回
     * "localhost"；InetAddress 只把本机地址缓存 5 秒。
     */
    private static void runtimeName(Path hostsFile, String hostname) throws Exception {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        System.out.println("identity-before=" + runtime.getPid() + "|" + runtime.getStartTime());
        Files.write(hostsFile, ("127.0.0.1 " + hostname + "\n").getBytes(StandardCharsets.UTF_8));
        System.out.println("resolved=" + runtime.getName());
        Files.write(hostsFile, "127.0.0.1 unrelated.invalid\n".getBytes(StandardCharsets.UTF_8));
        Thread.sleep(5_500L);
        System.out.println("unresolved=" + runtime.getName());
        System.out.println("identity-after=" + runtime.getPid() + "|" + runtime.getStartTime());
    }

    /** 主机名可解析与不可解析两种状态下，各重新派生一次 jvmId（不走缓存）。 */
    private static void jvmIdAcrossHostResolution(Path hostsFile, String hostname) throws Exception {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        Files.write(hostsFile, ("127.0.0.1 " + hostname + "\n").getBytes(StandardCharsets.UTF_8));
        System.out.println("resolved-name=" + runtime.getName());
        System.out.println("resolved-id=" + PaimonJvmIdentity.fromRuntime(runtime));
        Files.write(hostsFile, "127.0.0.1 unrelated.invalid\n".getBytes(StandardCharsets.UTF_8));
        Thread.sleep(5_500L);
        System.out.println("unresolved-name=" + runtime.getName());
        System.out.println("unresolved-id=" + PaimonJvmIdentity.fromRuntime(runtime));
    }

    /** 不经名称解析读取本机主机名：Linux 读 procfs，其他平台执行 hostname。 */
    static String localHostName() throws Exception {
        Path procfs = Paths.get("/proc/sys/kernel/hostname");
        if (Files.isReadable(procfs)) {
            return new String(Files.readAllBytes(procfs), StandardCharsets.UTF_8).trim();
        }
        Process process = new ProcessBuilder("hostname").redirectErrorStream(true).start();
        String name = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        if (!process.waitFor(10, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            return "";
        }
        return process.exitValue() == 0 ? name : "";
    }

    static String testClassPath() {
        String classpath = System.getProperty("surefire.test.class.path");
        if (classpath == null || classpath.trim().isEmpty()) {
            classpath = System.getProperty("java.class.path");
        }
        if (classpath == null || classpath.trim().isEmpty()) {
            throw new IllegalStateException("Test runtime classpath is unavailable");
        }
        return classpath;
    }

    static Handle start(List<String> jvmOptions, String... args) throws IOException {
        List<String> command = new ArrayList<>();
        command.add(Paths.get(System.getProperty("java.home"), "bin", "java").toString());
        command.addAll(jvmOptions);
        command.add("-cp");
        command.add(testClassPath());
        command.add(SpillProbeProcess.class.getName());
        command.addAll(Arrays.asList(args));
        return new Handle(new ProcessBuilder(command).redirectErrorStream(true).start());
    }

    /** 启动 try-lock 探针并返回外部进程能否取得排他锁。 */
    static boolean otherProcessAcquires(Path lockFile) throws Exception {
        try (Handle probe = start(Collections.emptyList(), "try-lock", lockFile.toString())) {
            String line = probe.expectLine("acquired=");
            probe.awaitSuccess();
            return Boolean.parseBoolean(line.substring("acquired=".length()));
        }
    }

    /** 在另一个 JVM 中运行 develop 现有清理器，返回其输出行。 */
    static String legacyCleanupInOtherJvm(Path root) throws Exception {
        try (Handle cleaner = start(Collections.emptyList(), "legacy-cleanup", root.toString())) {
            String line = cleaner.expectLine("deleted=");
            cleaner.awaitSuccess();
            return line;
        }
    }

    static final class Handle implements AutoCloseable {
        private static final long DEADLINE_SECONDS = 30L;
        private final Process process;
        private final BufferedReader output;
        private final StringBuilder transcript = new StringBuilder();

        private Handle(Process process) {
            this.process = process;
            this.output = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        }

        /** 读取到以 prefix 开头的行为止；子进程退出或超时都视为失败，不无限阻塞。 */
        String expectLine(String prefix) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(DEADLINE_SECONDS);
            while (System.nanoTime() < deadline) {
                if (output.ready()) {
                    String line = output.readLine();
                    if (line == null) {
                        break;
                    }
                    transcript.append(line).append('\n');
                    if (line.startsWith(prefix)) {
                        return line;
                    }
                } else if (!process.isAlive() && !output.ready()) {
                    break;
                } else {
                    Thread.sleep(10L);
                }
            }
            throw new AssertionError("Probe did not print '" + prefix + "', alive=" + process.isAlive()
                    + ", output:\n" + transcript);
        }

        void send(String line) throws IOException {
            OutputStream input = process.getOutputStream();
            input.write((line + "\n").getBytes(StandardCharsets.UTF_8));
            input.flush();
        }

        void awaitSuccess() throws Exception {
            if (!process.waitFor(DEADLINE_SECONDS, TimeUnit.SECONDS)) {
                throw new AssertionError("Probe did not exit, output:\n" + transcript);
            }
            if (process.exitValue() != 0) {
                StringBuilder rest = new StringBuilder();
                String line;
                while ((line = output.readLine()) != null) {
                    rest.append(line).append('\n');
                }
                throw new AssertionError("Probe exit=" + process.exitValue() + ", output:\n" + transcript + rest);
            }
        }

        @Override
        public void close() throws Exception {
            // 只回收本 Handle 启动的子进程。
            if (process.isAlive()) {
                process.destroyForcibly();
            }
            try {
                if (!process.waitFor(DEADLINE_SECONDS, TimeUnit.SECONDS)) {
                    throw new AssertionError("Probe process did not terminate");
                }
            } finally {
                process.getOutputStream().close();
                output.close();
            }
        }
    }
}
