package io.tapdata.tdd.run.support;

import io.tapdata.pdk.apis.TapConnector;
import io.tapdata.pdk.core.api.ConnectorNode;
import io.tapdata.tdd.run.base.PDKBaseRun;
import io.tapdata.tdd.run.base.RunnerSummary;
import io.tapdata.tdd.tdd.core.SupportFunction;
import io.tapdata.tdd.tdd.core.base.TestNode;
import io.tapdata.tdd.tdd.tests.support.TapGo;
import io.tapdata.tdd.tdd.tests.support.TapTestCase;
import io.tapdata.tdd.tdd.tests.basic.RecordEventExecute;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
@DisplayName("tableCountRun")
@TapGo(sort = 3)
public class TableCountRun extends PDKBaseRun {
    @DisplayName("tableCountRun.run")
    @TapTestCase(sort = 1)
    @Test
    public void tableCount() throws NoSuchMethodException {
        Method testCase = super.getMethod("tableCount");
        consumeQualifiedTapNodeInfo(nodeInfo -> {
            TestNode prepare = prepare(nodeInfo);
            RecordEventExecute execute = prepare.recordEventExecute();
            try {
                super.connectorOnStart(prepare);
                execute.testCase(testCase);
                ConnectorNode connectorNode = prepare.connectorNode();
                TapConnector connector = connectorNode.getConnector();
                long count = connector.tableCount(connectorNode.getConnectorContext());
                super.runSucceed(testCase, RunnerSummary.format("tableCountRun.succeed", count));
            } catch (Throwable exception) {
                super.runError(testCase, RunnerSummary.format("formatValue", exception.getMessage()));
            } finally {
                super.connectorOnStop(prepare);
            }
        });
    }

    public static List<SupportFunction> testFunctions() {
        return new ArrayList<>();
    }
}
