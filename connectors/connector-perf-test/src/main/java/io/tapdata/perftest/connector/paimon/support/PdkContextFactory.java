package io.tapdata.perftest.connector.paimon.support;

import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import org.mockito.Mockito;

/**
 * Builds minimal PDK context objects without a full Engine runtime.
 * Reuses the same Mockito-based TapNodeSpecification pattern as PerformanceTestRunner.
 */
public class PdkContextFactory {

    public static TapConnectionContext buildConnectionContext(DataMap connectionConfig) {
        TapNodeSpecification spec = Mockito.mock(TapNodeSpecification.class);
        return new TapConnectionContext(spec, connectionConfig, DataMap.create(), buildConsoleLog());
    }

    public static TapConnectorContext buildConnectorContext(DataMap connectionConfig, TapTable table) {
        TapNodeSpecification spec = Mockito.mock(TapNodeSpecification.class);
        TapConnectorContext ctx = new TapConnectorContext(spec, connectionConfig, DataMap.create(), buildConsoleLog());

        SimpleKVReadOnlyMap<TapTable> tableMap = new SimpleKVReadOnlyMap<>();
        if (table != null) tableMap.put(table.getId(), table);
        ctx.setTableMap(tableMap);
        ctx.setStateMap(new SimpleKVMap<>());
        ctx.setGlobalStateMap(new SimpleKVMap<>());
        return ctx;
    }

    public static void updateTableMap(TapConnectorContext ctx, TapTable table) {
        if (ctx.getTableMap() instanceof SimpleKVReadOnlyMap) {
            ((SimpleKVReadOnlyMap<TapTable>) ctx.getTableMap()).put(table.getId(), table);
        }
    }

    private static Log buildConsoleLog() {
        return new Log() {
            @Override public void debug(String m, Object... p) { print("[DEBUG]", m, p); }
            @Override public void info(String m, Object... p)  { print("[INFO] ", m, p); }
            @Override public void warn(String m, Object... p)  { print("[WARN] ", m, p); }
            @Override public void error(String m, Object... p) { print("[ERROR]", m, p); }
            @Override public void error(String m, Throwable t) {
                System.err.println("[ERROR] " + m + (t != null ? ": " + t.getMessage() : ""));
            }
            @Override public void fatal(String m, Object... p) { print("[FATAL]", m, p); }
            @Override public void trace(String m, Object... p) {}
            private void print(String prefix, String m, Object[] p) {
                if (p != null) { for (Object o : p) m = m.replaceFirst("\\{}", String.valueOf(o)); }
                System.out.println(prefix + " " + m);
            }
        };
    }
}
