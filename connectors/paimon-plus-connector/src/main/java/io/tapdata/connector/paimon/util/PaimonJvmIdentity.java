package io.tapdata.connector.paimon.util;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.regex.Pattern;

/**
 * JVM 实例标识（SPEC-paimon-spill-owner-isolation §4.2）。
 *
 * <p>{@code jvmId = "j" + hex(SHA-256("tapdata-paimon-v2|" + pid + "|" + startTimeMillis))[0:32]}。
 * 两个输入在进程生命周期内不变，且与 ClassLoader 无关，因此同一 JVM 的所有插件 ClassLoader 得到同一值，
 * 进程重启后值改变。
 *
 * <p>禁止使用 {@link RuntimeMXBean#getName()} 的主机名部分：JDK 17 {@code VMManagementImpl.getVmId}
 * 每次调用都经 {@code InetAddress.getLocalHost()} 解析主机名，失败时退回 {@code "localhost"}，同一 JVM 内
 * 结果会变化（PaimonSpillPlatformContractTest C3）。
 *
 * <p>派生函数与命名规则属于协议：任何改动都必须同时升级 {@code tapdata-paimon-v2} 子根版本，否则同一 JVM
 * 内新旧版本 ClassLoader 会把对方命名空间当作外部命名空间去探测锁文件。
 */
public final class PaimonJvmIdentity {

    static final String DERIVATION_DOMAIN = "tapdata-paimon-v2|";
    private static final Pattern NAMESPACE_NAME = Pattern.compile("j[0-9a-f]{32}");
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private static volatile String current;

    private PaimonJvmIdentity() {
    }

    /** 本 JVM 的标识；每个 ClassLoader 惰性计算一次。获取失败时抛出，调用方不得回退旧布局。 */
    public static String current() {
        String value = current;
        if (value == null) {
            synchronized (PaimonJvmIdentity.class) {
                value = current;
                if (value == null) {
                    value = fromRuntime(ManagementFactory.getRuntimeMXBean());
                    current = value;
                }
            }
        }
        return value;
    }

    static String fromRuntime(RuntimeMXBean runtime) {
        return derive(pidOf(runtime), runtime.getStartTime());
    }

    /**
     * JDK ≥ 10 通过反射调用 {@code RuntimeMXBean#getPid()}（默认实现为 {@code ProcessHandle.current().pid()}，
     * 不做名称解析）；生产代码以 release 8 编译，不能直接引用该方法。Java 8 运行时退回 {@code getName()}
     * 中第一个 {@code '@'} 之前的十进制串，该部分来自本地进程号，与主机名无关。
     * https://docs.oracle.com/en/java/javase/17/docs/api/java.management/java/lang/management/RuntimeMXBean.html#getPid()
     */
    static long pidOf(RuntimeMXBean runtime) {
        Method getPid;
        try {
            getPid = RuntimeMXBean.class.getMethod("getPid");
        } catch (NoSuchMethodException java8) {
            return pidFromName(runtime.getName());
        }
        try {
            Object pid = getPid.invoke(runtime);
            if (!(pid instanceof Long) || (Long) pid <= 0L) {
                throw new IllegalStateException("RuntimeMXBean.getPid returned an invalid process id");
            }
            return (Long) pid;
        } catch (InvocationTargetException failure) {
            throw new IllegalStateException("Cannot read JVM process id for the Paimon spill namespace",
                    failure.getCause());
        } catch (IllegalAccessException | RuntimeException failure) {
            throw new IllegalStateException("Cannot read JVM process id for the Paimon spill namespace", failure);
        }
    }

    static long pidFromName(String runtimeName) {
        int at = runtimeName == null ? -1 : runtimeName.indexOf('@');
        if (at <= 0) {
            throw new IllegalStateException("Unrecognized RuntimeMXBean name; expected <pid>@<host>");
        }
        try {
            long pid = Long.parseLong(runtimeName.substring(0, at));
            if (pid <= 0L) {
                throw new IllegalStateException("RuntimeMXBean name carries a non-positive process id");
            }
            return pid;
        } catch (NumberFormatException failure) {
            throw new IllegalStateException("RuntimeMXBean name carries a non-numeric process id", failure);
        }
    }

    static String derive(long pid, long startTimeMillis) {
        if (pid <= 0L || startTimeMillis <= 0L) {
            throw new IllegalArgumentException("pid and JVM start time must be positive");
        }
        byte[] digest;
        try {
            digest = MessageDigest.getInstance("SHA-256").digest(
                    (DERIVATION_DOMAIN + pid + "|" + startTimeMillis).getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException impossible) {
            // SHA-256 是每个 Java 平台实现必须提供的算法（java.security.MessageDigest 类文档）。
            throw new IllegalStateException(impossible);
        }
        StringBuilder id = new StringBuilder(33).append('j');
        for (int i = 0; i < 16; i++) {
            id.append(HEX[(digest[i] >> 4) & 0xF]).append(HEX[digest[i] & 0xF]);
        }
        return id.toString();
    }

    /** 回收者只把符合该格式的目录当作进程命名空间。 */
    static boolean isNamespaceName(String name) {
        return name != null && NAMESPACE_NAME.matcher(name).matches();
    }
}
