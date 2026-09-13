package io.tapdata.connector.custom;

import io.tapdata.connector.custom.config.CustomConfig;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.script.ScriptFactory;
import io.tapdata.entity.utils.BeanUtils;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.entity.utils.TapUtils;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.CommandResult;
import io.tapdata.pdk.apis.entity.message.CommandInfo;
import io.tapdata.pdk.apis.spec.TapNodeSpecification;
import io.tapdata.pdk.apis.utils.TypeConverter;
import io.tapdata.pdk.core.api.impl.serialize.ObjectSerializableImplV2;
import io.tapdata.script.factory.TapdataScriptFactory;
import io.tapdata.script.factory.script.TapRunScriptEngine;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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

	@Nested
	@DisplayName("Method testRun command test")
	class testRunTest {

		private BeanUtils beanUtils;

		@BeforeEach
		void setUpBeanUtils() {
			beanUtils = (BeanUtils) ReflectionTestUtils.getField(CustomConfig.class, "beanUtils");
			if (beanUtils == null) {
				beanUtils = mock(BeanUtils.class);
			}
			when(beanUtils.mapToBean(any(), any())).thenAnswer(invocation -> {
				Map<String, Object> map = invocation.getArgument(0);
				Object target = invocation.getArgument(1);
				if (map != null && target instanceof CustomConfig) {
					CustomConfig cfg = (CustomConfig) target;
					Optional.ofNullable((String) map.get("targetScript")).ifPresent(cfg::setTargetScript);
					Optional.ofNullable((String) map.get("jsEngineName")).ifPresent(cfg::setJsEngineName);
					Optional.ofNullable((String) map.get("collectionName")).ifPresent(cfg::setCollectionName);
					if (map.get("customBeforeOpr") instanceof Boolean) {
						cfg.setCustomBeforeOpr((Boolean) map.get("customBeforeOpr"));
					}
					if (map.get("customAfterOpr") instanceof Boolean) {
						cfg.setCustomAfterOpr((Boolean) map.get("customAfterOpr"));
					}
					if (map.get("batchProcess") instanceof Boolean) {
						cfg.setBatchProcess((Boolean) map.get("batchProcess"));
					}
				}
				return target;
			});
		}

		@Test
		@Timeout(15)
		@DisplayName("timeout should interrupt script thread instead of Thread.stop")
		void timeoutShouldNotUseThreadStop() {
			Log log = mock(Log.class);
			TapConnectionContext connectionContext = new TapConnectionContext(mock(TapNodeSpecification.class), DataMap.create(), DataMap.create(), log);
			CommandInfo commandInfo = new CommandInfo();
			commandInfo.setCommand("testRun");
			commandInfo.setType("target");
			Map<String, Object> connectionConfig = new HashMap<>();
			connectionConfig.put("jsEngineName", TapRunScriptEngine.EngineType.GRAALVM_JS.engineName());
			connectionConfig.put("collectionName", "a");
			connectionConfig.put("customBeforeOpr", false);
			connectionConfig.put("customAfterOpr", false);
			connectionConfig.put("batchProcess", false);
			connectionConfig.put("targetScript", "while(true){}");
			commandInfo.setConnectionConfig(connectionConfig);
			Map<String, Object> argMap = new HashMap<>();
			argMap.put("timeout", 1);
			List<Map<String, Object>> input = new ArrayList<>();
			Map<String, Object> insert = new HashMap<>();
			insert.put("op", "i");
			insert.put("table", "a");
			Map<String, Object> after = new HashMap<>();
			after.put("A", 1);
			insert.put("after", after);
			input.add(insert);
			argMap.put("input", input);
			commandInfo.setArgMap(argMap);

			CommandResult result = ReflectionTestUtils.invokeMethod(customConnector, "handleCommand", connectionContext, commandInfo);

			assertNotNull(result);
			assertNotNull(result.getData());
			@SuppressWarnings("unchecked")
			List<Object> logs = (List<Object>) result.getData();
			assertFalse(logs.isEmpty());
			boolean timeoutLogged = logs.stream().anyMatch(item -> String.valueOf(item).contains("timed out")
					|| String.valueOf(ReflectionTestUtils.getField(item, "message")).contains("timed out"));
			assertTrue(timeoutLogged);
			assertFalse(Thread.getAllStackTraces().keySet().stream()
					.anyMatch(thread -> thread.getName() != null && thread.getName().startsWith("CustomConnector-Test-Runner-") && thread.isAlive()));
		}

		@Test
		@Timeout(10)
		@DisplayName("successful target testRun should complete without Thread.stop")
		void successfulTargetTestRun() {
			Log log = mock(Log.class);
			TapConnectionContext connectionContext = new TapConnectionContext(mock(TapNodeSpecification.class), DataMap.create(), DataMap.create(), log);
			CommandInfo commandInfo = new CommandInfo();
			commandInfo.setCommand("testRun");
			commandInfo.setType("target");
			Map<String, Object> connectionConfig = new HashMap<>();
			connectionConfig.put("jsEngineName", TapRunScriptEngine.EngineType.GRAALVM_JS.engineName());
			connectionConfig.put("collectionName", "a");
			connectionConfig.put("customBeforeOpr", false);
			connectionConfig.put("customAfterOpr", false);
			connectionConfig.put("batchProcess", false);
			connectionConfig.put("targetScript", "");
			commandInfo.setConnectionConfig(connectionConfig);
			Map<String, Object> argMap = new HashMap<>();
			argMap.put("timeout", 5);
			List<Map<String, Object>> input = new ArrayList<>();
			Map<String, Object> insert = new HashMap<>();
			insert.put("op", "i");
			insert.put("table", "a");
			Map<String, Object> after = new HashMap<>();
			after.put("A", 1);
			insert.put("after", after);
			input.add(insert);
			argMap.put("input", input);
			commandInfo.setArgMap(argMap);

			CommandResult result = ReflectionTestUtils.invokeMethod(customConnector, "handleCommand", connectionContext, commandInfo);

			assertNotNull(result);
			assertNotNull(result.getData());
		}
	}
}