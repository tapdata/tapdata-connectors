package io.tapdata.connector.mysql;

import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.connector.mysql.util.MysqlUtil;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class MysqlJdbcLoggingTest {
    @Test
    void fileLoggingUsesUppercaseJdbcAndPreservesConnectorId() throws Throwable {
        verifyStartupLogging(true);
    }

    @Test
    void disabledFileLoggingDoesNotStartJdbcLogging() throws Throwable {
        verifyStartupLogging(false);
    }

    private void verifyStartupLogging(boolean fileLog) throws Throwable {
        TapConnectionContext context = mock(TapConnectionContext.class);
        Log log = mock(Log.class);
        when(context.getConnectionConfig()).thenReturn(new DataMap());
        when(context.getNodeConfig()).thenReturn(new DataMap());
        when(context.getLog()).thenReturn(log);
        MysqlConnector connector = new MysqlConnector();
        ReflectionTestUtils.setField(connector, "firstConnectorId", "jdbc-log-regression");
        RuntimeException stopBeforeDatabase = new RuntimeException("stop before database setup");

        try (MockedConstruction<MysqlConfig> configs = mockConstruction(MysqlConfig.class, (config, construction) -> {
            when(config.load(anyMap())).thenReturn(config);
            when(config.getFileLog()).thenReturn(fileLog);
        }); MockedStatic<MysqlUtil> mysqlUtil = mockStatic(MysqlUtil.class)) {
            mysqlUtil.when(() -> MysqlUtil.buildContextMapForMasterSlave(any(MysqlConfig.class)))
                    .thenThrow(stopBeforeDatabase);
            assertSame(stopBeforeDatabase, assertThrows(RuntimeException.class, () -> connector.onStart(context)));
            MysqlConfig config = configs.constructed().get(0);
            if (fileLog) {
                verify(log).info("Starting JDBC Logging, connectorId: {}", "jdbc-log-regression");
                verify(config).startJdbcLog("jdbc-log-regression");
            } else {
                verifyNoInteractions(log);
                verify(config, never()).startJdbcLog(any());
            }
        }
    }
}
