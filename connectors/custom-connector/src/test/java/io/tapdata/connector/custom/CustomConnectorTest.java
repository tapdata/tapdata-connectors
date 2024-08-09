package io.tapdata.connector.custom;

import io.tapdata.connector.custom.config.CustomConfig;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.utils.TypeConverter;
import io.tapdata.pdk.core.api.impl.serialize.ObjectSerializableImplV2;
import io.tapdata.script.factory.TapdataScriptFactory;
import io.tapdata.script.factory.script.TapRunScriptEngine;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-08-08 16:34
 **/
class CustomConnectorTest {

	private MockedStatic<InstanceFactory> instanceFactoryMockedStatic;
	private CustomConnector customConnector;

	@BeforeEach
	void setUp() {
		instanceFactoryMockedStatic = mockStatic(InstanceFactory.class);
		TapdataScriptFactory tapdataScriptFactory = new TapdataScriptFactory();
		instanceFactoryMockedStatic.when(() -> InstanceFactory.instance(ScriptFactory.class, "engine")).thenReturn(tapdataScriptFactory);
		instanceFactoryMockedStatic.when(() -> InstanceFactory.instance(TypeConverter.class)).thenReturn(mock(TypeConverter.class));
		instanceFactoryMockedStatic.when(() -> InstanceFactory.instance(TapUtils.class)).thenReturn(mock(TapUtils.class));
		instanceFactoryMockedStatic.when(() -> InstanceFactory.instance(BeanUtils.class)).thenReturn(mock(BeanUtils.class));
		instanceFactoryMockedStatic.when(() -> InstanceFactory.instance(ObjectSerializable.class)).thenReturn(new ObjectSerializableImplV2());
		customConnector = spy(new CustomConnector());
		doReturn(true).when(customConnector).isAlive();
	}

	@AfterEach
	void tearDown() {
		Optional.ofNullable(instanceFactoryMockedStatic).ifPresent(MockedStatic::close);
	}

	@Nested
	@DisplayName("Method batchRead test")
	class batchReadTest {

		private TapConnectorContext tapConnectorContext;
		private int count;

		@BeforeEach
		void setUp() {
			tapConnectorContext = mock(TapConnectorContext.class);
			Log log = mock(Log.class);
			when(tapConnectorContext.getLog()).thenReturn(log);
			CustomConfig customConfig = new CustomConfig();
			customConfig.setJsEngineName(TapRunScriptEngine.EngineType.GRAALVM_JS.name());
			count = 123;
			customConfig.setHistoryScript("for(let i = 0;i<" + count + ";i++){\n" +
					"  let id = i+1\n" +
					"  core.push({\"id\": id, \"title\": \"xxxx\", \"created\": new Date()})  \n" +
					"}");
			ReflectionTestUtils.setField(customConnector, "customConfig", customConfig);
		}

		@Test
		@DisplayName("test main process")
		void test1() {
			List<TapEvent> result = new ArrayList<>();
			BiConsumer<List<TapEvent>, Object> biConsumer = (events, offsetObj) -> result.addAll(events);
			TapTable tapTable = new TapTable("test");

			assertDoesNotThrow(() -> customConnector.batchRead(tapConnectorContext, tapTable, null, 10, biConsumer));

			assertEquals(count, result.size());
		}
	}
}