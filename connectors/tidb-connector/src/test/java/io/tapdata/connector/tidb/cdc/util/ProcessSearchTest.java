package io.tapdata.connector.tidb.cdc.util;

import io.tapdata.entity.logger.Log;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

class ProcessSearchTest {
    Log log;
    @BeforeEach
    void init() {
        log = mock(Log.class);
    }
    @Test
    void testGetCommand() {
        String property = System.getProperty("os.name");
        try {
            try {
                System.setProperty("os.name", "win");
                String[] cmd = ProcessSearch.getCommand("cmd");
                Assertions.assertEquals(3, cmd.length);
                Assertions.assertEquals("cmd.exe", cmd[0]);
                Assertions.assertEquals("/c", cmd[1]);
                Assertions.assertEquals("cmd", cmd[2]);
            } finally {
                System.setProperty("os.name", property);
            }

            try {
                System.setProperty("os.name", "mac");
                String[] cmd = ProcessSearch.getCommand("cmd");
                Assertions.assertEquals(3, cmd.length);
                Assertions.assertEquals("/bin/sh", cmd[0]);
                Assertions.assertEquals("-c", cmd[1]);
                Assertions.assertEquals("cmd", cmd[2]);
            } finally {
                System.setProperty("os.name", property);
            }
        } finally {
            Assertions.assertEquals(property, System.getProperty("os.name"));
        }
    }

    @Nested
    class getSearchCommand{
        @Test
        void testWin() {
            String property = System.getProperty("os.name");
            try {
                System.setProperty("os.name", "win");
                String cmd = ProcessSearch.getSearchCommand(log, "cmd");
                Assertions.assertEquals("tasklist", cmd);
            } finally {
                System.setProperty("os.name", property);
                Assertions.assertEquals(property, System.getProperty("os.name"));
            }
        }
        @Test
        void testNix() {
            String property = System.getProperty("os.name");
            try {
                System.setProperty("os.name", "nix");
                String cmd = ProcessSearch.getSearchCommand(log, "cmd");
                Assertions.assertEquals("ps -e | grep cmd | grep -v grep ", cmd);
            } finally {
                System.setProperty("os.name", property);
                Assertions.assertEquals(property, System.getProperty("os.name"));
            }
        }
        @Test
        void testNux() {
            String property = System.getProperty("os.name");
            try {
                System.setProperty("os.name", "nux");
                String cmd = ProcessSearch.getSearchCommand(log, "cmd");
                Assertions.assertEquals("ps -e | grep cmd | grep -v grep ", cmd);
            } finally {
                System.setProperty("os.name", property);
                Assertions.assertEquals(property, System.getProperty("os.name"));
            }
        }
        @Test
        void testMac() {
            String property = System.getProperty("os.name");
            try {
                System.setProperty("os.name", "mac");
                String cmd = ProcessSearch.getSearchCommand(log, "cmd");
                Assertions.assertEquals("ps -e | grep cmd | grep -v grep ", cmd);
            } finally {
                System.setProperty("os.name", property);
                Assertions.assertEquals(property, System.getProperty("os.name"));
            }
        }
        @Test
        void testOther() {
            doNothing().when(log).warn("Unsupported operating system: {}", "xxx");
            String property = System.getProperty("os.name");
            try {
                System.setProperty("os.name", "xxx");
                String cmd = ProcessSearch.getSearchCommand(log, "cmd");
                Assertions.assertNull(cmd);
            } finally {
                System.setProperty("os.name", property);
                Assertions.assertEquals(property, System.getProperty("os.name"));
            }
        }
        @Test
        void testMany() {
            String property = System.getProperty("os.name");
            try {
                System.setProperty("os.name", "mac");
                String cmd = ProcessSearch.getSearchCommand(log, "cmd", "cmd");
                Assertions.assertEquals("ps -e | awk cmd && cmd", cmd);
            } finally {
                System.setProperty("os.name", property);
                Assertions.assertEquals(property, System.getProperty("os.name"));
            }
        }
        @Test
        void testNone() {
            String property = System.getProperty("os.name");
            try {
                System.setProperty("os.name", "mac");
                String cmd = ProcessSearch.getSearchCommand(log);
                Assertions.assertEquals("ps -e", cmd);
            } finally {
                System.setProperty("os.name", property);
                Assertions.assertEquals(property, System.getProperty("os.name"));
            }
        }
        @Test
        void testNull() {
            String property = System.getProperty("os.name");
            try {
                System.setProperty("os.name", "mac");
                String cmd = ProcessSearch.getSearchCommand(log, null);
                Assertions.assertEquals("ps -e", cmd);
            } finally {
                System.setProperty("os.name", property);
                Assertions.assertEquals(property, System.getProperty("os.name"));
            }
        }
    }

}