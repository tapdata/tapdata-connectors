package io.tapdata.connector.kafka;

import io.tapdata.connector.kafka.admin.DefaultAdmin;
import io.tapdata.connector.kafka.config.KafkaConfig;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.springframework.test.util.ReflectionTestUtils;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class KafkaAdminCancellationTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void adminFailuresCloseClientAndOnlyCancellationSetsInterrupt(boolean interrupted) {
        KafkaConnector connector = new KafkaConnector();
        KafkaConfig config = new KafkaConfig();
        config.setTopicName("orders");
        ReflectionTestUtils.setField(connector, "kafkaConfig", config);
        Exception failure = interrupted ? new InterruptedException("topic creation canceled")
                : new ExecutionException(new IllegalStateException("broker rejected topic"));
        try (MockedConstruction<DefaultAdmin> admins = mockConstruction(DefaultAdmin.class, (admin, context) -> {
            when(admin.listTopics()).thenReturn(Collections.emptySet());
            doThrow(failure).when(admin).createTopics(eq("orders"), anyInt(), anyShort());
        })) {
            RuntimeException result = assertThrows(RuntimeException.class, () -> ReflectionTestUtils.invokeMethod(
                    connector, "createTableV2", mock(TapConnectorContext.class), new TapCreateTableEvent()));
            assertTrue(result.getMessage().contains("Create Table orders Failed"));
            assertEquals(interrupted, Thread.currentThread().isInterrupted());
            assertEquals(1, admins.constructed().size());
            verify(admins.constructed().get(0)).close();
        } finally {
            Thread.interrupted();
        }
    }
}
