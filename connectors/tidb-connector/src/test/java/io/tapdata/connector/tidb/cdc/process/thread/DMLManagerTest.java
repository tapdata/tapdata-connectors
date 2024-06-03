package io.tapdata.connector.tidb.cdc.process.thread;


import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

class DMLManagerTest {
    class Context extends TapConnectorContext {
        public Context(TapNodeSpecification specification, DataMap connectionConfig, DataMap nodeConfig, Log log) {
            super(specification, connectionConfig, nodeConfig, log);
        }

    }

    class Consumer extends StreamReadConsumer {
        @Override
        public void accept(List<TapEvent> events, Object offset) {
            StringBuilder builder = new StringBuilder();
            builder.append("\n=====\n");
            builder.append(TapSimplify.toJson(events));
            builder.append("\n=====\n\n");
            System.out.println(builder.toString());
        }
    }

    @Test
    void testDMLRead() throws Exception {
        Context context = new Context(null, new DataMap(), new DataMap(), new Log() {
            @Override
            public void debug(String s, Object... objects) {

            }

            @Override
            public void info(String s, Object... objects) {

            }

            @Override
            public void warn(String s, Object... objects) {

            }

            @Override
            public void error(String s, Object... objects) {

            }

            @Override
            public void error(String s, Throwable throwable) {

            }

            @Override
            public void fatal(String s, Object... objects) {

            }
        });
        Consumer consumer = new Consumer();
        ProcessHandler.ProcessInfo info = new ProcessHandler.ProcessInfo()
                .withCdcServer("127.0.0.1:8300")
                .withFeedId(UUID.randomUUID().toString().replaceAll("-", ""))
                .withAlive(() -> true)
                .withTapConnectorContext(context)
                .withCdcTable(null)
                .withThrowableCollector(new AtomicReference<>())
                .withCdcOffset(new Object())
                .withDatabase("test");
        ProcessHandler handler = new ProcessHandler(info, consumer);
        handler.init();
        try {
            handler.doActivity();
            int times = 10000;
            while (times > 0) {
                Thread.sleep(100);
                times--;
            }
        } finally {
            handler.close();
        }
    }

    @Test
    void test() {
        Assertions.assertTrue("schema_450137783885627416_0206100104.json".matches("(schema_)([\\d]{18})(_)([\\d]{10})(\\.json)"));
    }
}