package org.apache.paimon.utils;

import org.junit.jupiter.api.Test;
import java.io.InputStream;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;
import static org.junit.jupiter.api.Assertions.*;

/** Fail the test phase on Paimon upgrade until the vendored implementation is reviewed. */
class PaimonHintOverrideCompatibilityTest {
    @Test
    void overrideIsPinnedAndMatchesUpstreamPublicApi() throws Exception {
        URL core = SnapshotManager.class.getProtectionDomain().getCodeSource().getLocation();
        try (JarFile jar = new JarFile(new java.io.File(core.toURI()))) {
            Properties version = new Properties();
            try (InputStream in = jar.getInputStream(jar.getJarEntry(
                    "META-INF/maven/org.apache.paimon/paimon-core/pom.properties"))) {
                version.load(in);
            }
            assertEquals("1.3.2", version.getProperty("version"),
                    "Review HintFileUtils against the new upstream source before changing this pin");
        }
        // Child-load only the original class; all method parameter types come from the parent.
        try (URLClassLoader upstream = new URLClassLoader(new URL[]{core}, HintFileUtils.class.getClassLoader()) {
            @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (!name.equals(HintFileUtils.class.getName())) { return super.loadClass(name, resolve); }
                synchronized (getClassLoadingLock(name)) {
                    Class<?> type = findLoadedClass(name);
                    if (type == null) { type = findClass(name); }
                    if (resolve) { resolveClass(type); }
                    return type;
                }
            }
        }) {
            Class<?> original = upstream.loadClass(HintFileUtils.class.getName());
            assertNotSame(HintFileUtils.class, original);
            assertEquals(api(original), api(HintFileUtils.class));
            assertNotEquals(core, HintFileUtils.class.getProtectionDomain().getCodeSource().getLocation(),
                    "Tests must load the connector override rather than upstream HintFileUtils");
        }
    }

    @Test
    void orphanWarningsAreLimitedPerDirectory() {
        String first = UUID.randomUUID().toString();
        String second = UUID.randomUUID().toString();
        assertTrue(HintFileUtils.shouldLogOrphan(first, 100));
        assertFalse(HintFileUtils.shouldLogOrphan(first, 101));
        assertTrue(HintFileUtils.shouldLogOrphan(second, 101));
        assertTrue(HintFileUtils.shouldLogOrphan(first, 100 + TimeUnit.MINUTES.toNanos(1)));
    }

    private Set<String> api(Class<?> type) {
        Set<String> signatures = new TreeSet<>();
        for (java.lang.reflect.Method m : type.getDeclaredMethods()) {
            if (Modifier.isPublic(m.getModifiers())) {
                signatures.add(m.getName() + Arrays.toString(m.getParameterTypes())
                        + m.getReturnType().getName() + Modifier.isStatic(m.getModifiers()));
            }
        }
        for (java.lang.reflect.Field f : type.getDeclaredFields()) {
            if (Modifier.isPublic(f.getModifiers())) {
                signatures.add(f.getName() + ":" + f.getType().getName() + ":" + f.getModifiers());
            }
        }
        return signatures;
    }
}
