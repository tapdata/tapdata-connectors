package io.tapdata.connector.paimon.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Proxy;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/** AC-01 / AC-08（身份部分）：jvmId 由 pid 与 JVM 启动时间派生，跨 ClassLoader 一致、跨进程不同、与主机名无关。 */
class PaimonJvmIdentityTest {

    /** 固定期望值守护协议：改动派生函数必须同时升级 tapdata-paimon-v2 子根（Spec §4.2）。 */
    @Test
    void derivationIsPinnedByGoldenValue() {
        // printf 'tapdata-paimon-v2|12345|1700000000000' | shasum -a 256 的前 32 位十六进制
        assertEquals("j23c8ef0021b69793a6daf38a4e67e0cb", PaimonJvmIdentity.derive(12345L, 1700000000000L));
    }

    @Test
    void namespaceNamePatternAcceptsOnlyDerivedShape() {
        assertTrue(PaimonJvmIdentity.isNamespaceName(PaimonJvmIdentity.derive(1L, 1L)));
        assertFalse(PaimonJvmIdentity.isNamespaceName("j23C8EF0021B69793A6DAF38A4E67E0CB"));
        assertFalse(PaimonJvmIdentity.isNamespaceName("j23c8ef0021b69793a6daf38a4e67e0c"));
        assertFalse(PaimonJvmIdentity.isNamespaceName("paimon-io-23c8ef0021b69793a6daf38a4e67e0cb"));
        assertFalse(PaimonJvmIdentity.isNamespaceName(null));
    }

    @Test
    void currentIsDerivedFromProcessIdAndStartTimeAndStable() {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        String expected = PaimonJvmIdentity.derive(ProcessHandle.current().pid(), runtime.getStartTime());
        assertEquals(expected, PaimonJvmIdentity.current());
        assertEquals(expected, PaimonJvmIdentity.current());
    }

    @Test
    void isolatedClassLoadersObserveTheSameIdentity() throws Exception {
        try (URLClassLoader first = IsolatedConnectorLoader.create();
             URLClassLoader second = IsolatedConnectorLoader.create()) {
            Class<?> a = first.loadClass(PaimonJvmIdentity.class.getName());
            Class<?> b = second.loadClass(PaimonJvmIdentity.class.getName());
            assertNotSame(a, b, "两个 loader 必须各自定义该类（各有一份 static 缓存）");
            assertNotSame(PaimonJvmIdentity.class, a);
            Object fromFirst = a.getMethod("current").invoke(null);
            Object fromSecond = b.getMethod("current").invoke(null);
            assertEquals(PaimonJvmIdentity.current(), fromFirst);
            assertEquals(fromFirst, fromSecond);
        }
    }

    @Test
    void anotherProcessObservesADifferentIdentity() throws Exception {
        try (SpillProbeProcess.Handle child = SpillProbeProcess.start(Collections.emptyList(), "jvm-id")) {
            String line = child.expectLine("jvmId=");
            child.awaitSuccess();
            String childId = line.substring("jvmId=".length());
            assertTrue(PaimonJvmIdentity.isNamespaceName(childId));
            assertNotEquals(PaimonJvmIdentity.current(), childId);
        }
    }

    /** 与 C3 同一手段：主机名解析结果在同一 JVM 内变化时，getName() 改变，而重新派生的 jvmId 不变。 */
    @Test
    @DisabledOnOs(OS.WINDOWS)
    void identityIgnoresHostNameResolution(@TempDir Path dir) throws Exception {
        String hostname = SpillProbeProcess.localHostName();
        assumeFalse(hostname.isEmpty() || "localhost".equals(hostname), "主机名为 localhost 时无法制造差异");
        Path hosts = Files.write(dir.resolve("hosts"), new byte[0]);
        try (SpillProbeProcess.Handle child = SpillProbeProcess.start(
                Collections.singletonList("-Djdk.net.hosts.file=" + hosts),
                "jvm-id-hosts", hosts.toString(), hostname)) {
            String resolvedName = value(child.expectLine("resolved-name="));
            String resolvedId = value(child.expectLine("resolved-id="));
            String unresolvedName = value(child.expectLine("unresolved-name="));
            String unresolvedId = value(child.expectLine("unresolved-id="));
            child.awaitSuccess();
            assertNotEquals(resolvedName, unresolvedName, "前提：两次 getName() 必须不同");
            assertTrue(unresolvedName.endsWith("@localhost"));
            assertEquals(resolvedId, unresolvedId, "jvmId 不得受主机名解析影响");
        }
    }

    @Test
    void java8NameFallbackParsesOnlyTheProcessIdPrefix() {
        assertEquals(4321L, PaimonJvmIdentity.pidFromName("4321@any-host"));
        assertEquals(4321L, PaimonJvmIdentity.pidFromName("4321@localhost"));
        assertThrows(IllegalStateException.class, () -> PaimonJvmIdentity.pidFromName("host-only"));
        assertThrows(IllegalStateException.class, () -> PaimonJvmIdentity.pidFromName("@host"));
        assertThrows(IllegalStateException.class, () -> PaimonJvmIdentity.pidFromName("abc@host"));
        assertThrows(IllegalStateException.class, () -> PaimonJvmIdentity.pidFromName("0@host"));
        assertThrows(IllegalStateException.class, () -> PaimonJvmIdentity.pidFromName(null));
    }

    @Test
    void unreadableProcessIdFailsClosed() {
        RuntimeMXBean throwing = runtime(new IllegalStateException("denied"), 1L);
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> PaimonJvmIdentity.fromRuntime(throwing));
        assertEquals("denied", failure.getCause().getMessage());
        assertThrows(IllegalStateException.class, () -> PaimonJvmIdentity.fromRuntime(runtime(null, -1L)));
        assertThrows(IllegalArgumentException.class, () -> PaimonJvmIdentity.derive(1L, 0L));
    }

    private static RuntimeMXBean runtime(RuntimeException pidFailure, long pid) {
        return (RuntimeMXBean) Proxy.newProxyInstance(PaimonJvmIdentityTest.class.getClassLoader(),
                new Class<?>[] {RuntimeMXBean.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getPid":
                            if (pidFailure != null) { throw pidFailure; }
                            return pid;
                        case "getStartTime":
                            return 1700000000000L;
                        case "getName":
                            return pid + "@proxy";
                        default:
                            throw new UnsupportedOperationException(method.getName());
                    }
                });
    }

    private static String value(String line) {
        return line.substring(line.indexOf('=') + 1);
    }
}
