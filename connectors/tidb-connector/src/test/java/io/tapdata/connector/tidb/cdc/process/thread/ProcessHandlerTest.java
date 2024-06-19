package io.tapdata.connector.tidb.cdc.process.thread;

import io.tapdata.connector.tidb.util.HttpUtil;
import io.tapdata.connector.tidb.util.pojo.ChangeFeed;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ProcessHandlerTest {
    ProcessHandler processHandler;
    ProcessHandler.ProcessInfo processInfo;
    TiCDCShellManager shellManager;

    TiCDCShellManager.ShellConfig shellConfig;
    Log log;
    @BeforeEach
    void init() {
        shellManager = mock(TiCDCShellManager.class);
        shellConfig = new TiCDCShellManager.ShellConfig();
        shellConfig.withCdcServerIpPort("");
        ReflectionTestUtils.setField(shellManager, "shellConfig", shellConfig);

        log = mock(Log.class);
        processInfo = new ProcessHandler.ProcessInfo();
        processInfo.withCdcServer("");
        processInfo.withFeedId("");
        processHandler = mock(ProcessHandler.class);
        ReflectionTestUtils.setField(processHandler, "processInfo", processInfo);
        ReflectionTestUtils.setField(processHandler, "log", log);
        ReflectionTestUtils.setField(processHandler, "shellManager", shellManager);
    }


    @Test
    void testCreateChangeFeed() throws IOException {
        HttpUtil httpUtil = mock(HttpUtil.class);
        ChangeFeed changefeed = mock(ChangeFeed.class);
        when(httpUtil.createChangeFeed(changefeed, processInfo.cdcServer)).thenReturn(true);
        doCallRealMethod().when(processHandler).createChangeFeed(httpUtil, changefeed, 0L);
        doNothing().when(log).info(anyString(), anyLong());
        Assertions.assertDoesNotThrow(() -> processHandler.createChangeFeed(httpUtil, changefeed, 0L));
    }
    @Test
    void testCreateChangeFeed1() throws IOException {
        HttpUtil httpUtil = mock(HttpUtil.class);
        ChangeFeed changefeed = mock(ChangeFeed.class);
        when(httpUtil.createChangeFeed(changefeed, processInfo.cdcServer)).thenReturn(false);
        doCallRealMethod().when(processHandler).createChangeFeed(httpUtil, changefeed, 0L);
        doNothing().when(log).info(anyString(), anyLong());
        Assertions.assertThrows(CoreException.class, () -> processHandler.createChangeFeed(httpUtil, changefeed, 0L));
    }
    @Test
    void testCheckAliveAndWait() throws IOException {
        HttpUtil httpUtil = mock(HttpUtil.class);
        ChangeFeed changefeed = mock(ChangeFeed.class);
        doNothing().when(processHandler).createChangeFeed(httpUtil, changefeed, 0L);
        doNothing().when(processHandler).sleep(anyLong());
        doNothing().when(log).warn(anyString(), anyString(), anyString());
        when(httpUtil.queryChangeFeedsList(anyString(), anyString())).thenReturn(false, false, false, false, false, false, false, false);
        doCallRealMethod().when(processHandler).checkAliveAndWait(httpUtil, changefeed, 0L);
        Assertions.assertDoesNotThrow(() -> processHandler.checkAliveAndWait(httpUtil, changefeed, 0L));
    }
    @Test
    void testCheckAliveAndWait1() throws IOException {
        HttpUtil httpUtil = mock(HttpUtil.class);
        ChangeFeed changefeed = mock(ChangeFeed.class);
        doNothing().when(processHandler).createChangeFeed(httpUtil, changefeed, 0L);
        doNothing().when(processHandler).sleep(anyLong());
        doNothing().when(log).warn(anyString(), anyString(), anyString());
        when(httpUtil.queryChangeFeedsList(anyString(), anyString())).thenReturn(true);
        doCallRealMethod().when(processHandler).checkAliveAndWait(httpUtil, changefeed, 0L);
        Assertions.assertDoesNotThrow(() -> processHandler.checkAliveAndWait(httpUtil, changefeed, 0L));
    }
    @Test
    void testSleep() {
        doCallRealMethod().when(processHandler).sleep(anyLong());
        try {
            processHandler.sleep(1);
        } catch (Exception e) {

        }
    }
}