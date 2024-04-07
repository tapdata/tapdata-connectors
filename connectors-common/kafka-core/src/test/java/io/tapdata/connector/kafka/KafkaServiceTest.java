package io.tapdata.connector.kafka;

import io.tapdata.common.MqConfig;
import io.tapdata.connector.kafka.config.KafkaConfig;
import io.tapdata.connector.kafka.util.ScriptUtil;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.utils.ClassFactory;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.JsonParser;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.script.factory.script.TapRunScriptEngine;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import javax.script.ScriptEngine;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

public class KafkaServiceTest {
    private KafkaService kafkaService;
    @Nested
    class Produce2MethodTest{
        private TapConnectorContext connectorContext;
        private List<TapRecordEvent> tapRecordEvents;
        private TapTable tapTable;
        private Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer;
        private Supplier<Boolean> isAlive;
        private KafkaProducer<byte[], byte[]> kafkaProducer;
        MqConfig mqConfig;
        @BeforeEach
        void buildParam(){
            kafkaService = mock(KafkaService.class);
            connectorContext = mock(TapConnectorContext.class);
            List<TapRecordEvent> list = new ArrayList<>();
            list.add(mock(TapInsertRecordEvent.class));
            tapRecordEvents = list;
            tapTable = mock(TapTable.class);
            writeListResultConsumer = mock(Consumer.class);
            isAlive = mock(Supplier.class);
            mqConfig = mock(KafkaConfig.class);
            ReflectionTestUtils.setField(kafkaService,"mqConfig",mqConfig);
            kafkaProducer = mock(KafkaProducer.class);
            ReflectionTestUtils.setField(kafkaService,"kafkaProducer",kafkaProducer);
        }
        @Test
        void testProduce() throws NoSuchFieldException, IllegalAccessException {
            ScriptFactory scriptFactory = mock(ScriptFactory.class);
            reflectScriptFactory(scriptFactory);
            String script = "record.data.createTime = new Date()";
            when(((KafkaConfig)mqConfig).getScript()).thenReturn(script);
            ScriptEngine scriptEngine = mock(TapRunScriptEngine.class);
            when(scriptFactory.create(anyString(),any())).thenReturn(scriptEngine);
            when(isAlive.get()).thenReturn(true).thenReturn(false);
            doCallRealMethod().when(kafkaService).produce(connectorContext,tapRecordEvents,tapTable,writeListResultConsumer,isAlive);
            kafkaService.produce(connectorContext,tapRecordEvents,tapTable,writeListResultConsumer,isAlive);
//            verify(kafkaService).initBuildInMethod();
        }
        private void reflectScriptFactory(ScriptFactory mockScriptFactory) throws NoSuchFieldException, IllegalAccessException {
            Field scriptFactory = KafkaService.class.getDeclaredField("scriptFactory");
            scriptFactory.setAccessible(true);
            Field modifiers = Field.class.getDeclaredField("modifiers");
            modifiers.setAccessible(true);
            modifiers.setInt(scriptFactory, scriptFactory.getModifiers() & ~Modifier.FINAL);
            scriptFactory.set(kafkaService, mockScriptFactory);
        }
    }
    @Nested
    class InitBuildInMethodTest{
        @Test
        void testInitBuildInMethod(){
            kafkaService = new KafkaService();
            String s = ScriptUtil.initBuildInMethod();
            assertEquals(true,s.contains("var DateUtil"));
        }
    }
}