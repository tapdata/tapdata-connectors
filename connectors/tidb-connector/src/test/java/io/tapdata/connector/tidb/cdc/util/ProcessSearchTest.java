package io.tapdata.connector.tidb.cdc.util;

import io.tapdata.entity.logger.Log;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

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
                Assertions.assertEquals("ps -ef | grep cmd | grep -v grep ", cmd);
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
                Assertions.assertEquals("ps -ef | grep cmd | grep -v grep ", cmd);
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
                Assertions.assertEquals("ps -ef | grep cmd | grep -v grep ", cmd);
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
                Assertions.assertEquals("ps -ef | awk cmd && cmd", cmd);
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
                Assertions.assertEquals("ps -ef", cmd);
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
                Assertions.assertEquals("ps -ef", cmd);
            } finally {
                System.setProperty("os.name", property);
                Assertions.assertEquals(property, System.getProperty("os.name"));
            }
        }
    }


    @Test
    void getPortFromLine() {
        Assertions.assertNull(ProcessSearch.getPortFromLine(""));
        Assertions.assertNotNull(ProcessSearch.getPortFromLine("0 83800  1046   0 12:56下午 ttys006    0:00.01 ps -ef"));
        Assertions.assertNull(ProcessSearch.getPortFromLine("0 x  1046   0 12:56下午 ttys006    0:00.01 ps -ef"));
        Assertions.assertNotNull(ProcessSearch.getPortFromLine("0     83800      1046   0 12:56下午 ttys006    0:00.01 ps -ef"));
        Assertions.assertNull(ProcessSearch.getPortFromLine("0"));
    }

    @Test
    void getProcessesPortsAsLine() {
        List<String> p = new ArrayList<>();
        p.add("");
        p.add("0 83800  1046   0 12:56下午 ttys006    0:00.01 ps -ef");
        try (MockedStatic<ProcessSearch> ps = mockStatic(ProcessSearch.class) ) {
            ps.when(() -> ProcessSearch.getProcesses(log, "")).thenReturn(p);
            ps.when(() -> ProcessSearch.getProcessesPortsAsLine(" ", log, "")).thenCallRealMethod();
            ps.when(() -> ProcessSearch.getPortFromLine(anyString())).thenCallRealMethod();
            Assertions.assertNotNull(ProcessSearch.getProcessesPortsAsLine(" ", log, ""));
        }
    }
}