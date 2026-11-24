package io.tapdata.oceanbase.runtime;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OceanbaseCdcRuntimeTest {

    @Test
    void shouldSelectSupportedPlatforms() {
        assertEquals("linux-x86_64", OceanbaseCdcRuntime.platform("Linux", "amd64"));
        assertEquals("linux-x86_64", OceanbaseCdcRuntime.platform("Linux", "x86_64"));
        assertEquals("linux-aarch64", OceanbaseCdcRuntime.platform("Linux", "aarch64"));
        assertEquals("linux-aarch64", OceanbaseCdcRuntime.platform("Linux", "arm64"));
        assertThrows(IllegalStateException.class, () -> OceanbaseCdcRuntime.platform("Windows", "amd64"));
        assertThrows(IllegalStateException.class, () -> OceanbaseCdcRuntime.platform("Linux", "arm"));
        assertThrows(IllegalStateException.class, () -> OceanbaseCdcRuntime.platform("Linux", "ppc64le"));
    }

    @Test
    void shouldRejectArchivePathTraversal() throws Exception {
        Path cache = Files.createTempDirectory("obcdc-runtime-test");
        Path escaped = cache.resolve("escaped");
        byte[] archive = archive("../../../../escaped", "bad");

        assertThrows(IOException.class, () -> OceanbaseCdcRuntime.prepare(new ByteArrayInputStream(archive), cache));
        assertFalse(Files.exists(escaped));
    }

    @Test
    void shouldRejectIncompleteRuntime() throws Exception {
        Path cache = Files.createTempDirectory("obcdc-runtime-test");
        byte[] archive = archive("etc/libobcdc.conf", "incomplete");

        assertThrows(IOException.class, () -> OceanbaseCdcRuntime.prepare(new ByteArrayInputStream(archive), cache));
    }

    private static byte[] archive(String name, String value) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
            zip.putNextEntry(new ZipEntry(name));
            zip.write(value.getBytes(StandardCharsets.UTF_8));
            zip.closeEntry();
        }
        return bytes.toByteArray();
    }
}
