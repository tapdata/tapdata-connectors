package io.tapdata.storage.sftp;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SftpConfigTest {

    @Test
    void defaultsToCompatibilityHostKeyCheckingAndUtf8() {
        SftpConfig config = new SftpConfig();

        assertEquals("UTF-8", config.getEncoding());
        assertEquals("no", config.getSftpStrictHostKeyChecking());
        assertEquals(10000, config.getSftpConnectionTimeoutMillis());
    }

    @Test
    void rejectsInvalidConnectionSettings() {
        SftpConfig config = new SftpConfig();
        config.setSftpHost("host");
        config.setSftpUsername("user");
        config.setSftpStrictHostKeyChecking("unsafe");

        assertThrows(IllegalArgumentException.class, config::validate);
    }

    @Test
    void rejectsStrictCheckingWithoutKnownHosts() {
        SftpConfig config = new SftpConfig();
        config.setSftpHost("host");
        config.setSftpUsername("user");
        config.setSftpStrictHostKeyChecking("yes");

        assertThrows(IllegalArgumentException.class, config::validate);
    }
}
