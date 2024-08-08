package io.tapdata.connector.custom;

import com.hazelcast.core.HazelcastInstance;
import com.tapdata.constant.HazelcastUtil;
import io.tapdata.connector.custom.config.CustomConfig;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.script.factory.script.TapRunScriptEngine;
import org.junit.jupiter.api.*;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.*;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author samuel
 * @Description
 * @create 2024-08-08 16:34
 **/
class CustomConnectorTest {

	private CustomConnector customConnector;

	@BeforeEach
	void setUp() {
		customConnector = spy(new CustomConnector());
		doReturn(true).when(customConnector).isAlive();
	}

	@Nested
	@DisplayName("Method batchRead test")
	class batchReadTest {

		private TapConnectorContext tapConnectorContext;
		private int count;
		private MockedStatic<HazelcastUtil> hazelcastUtilMockedStatic;

		@BeforeEach
		void setUp() {
			tapConnectorContext = mock(TapConnectorContext.class);
			Log log = mock(Log.class);
			when(tapConnectorContext.getLog()).thenReturn(log);
			hazelcastUtilMockedStatic = mockStatic(HazelcastUtil.class);
			HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
			hazelcastUtilMockedStatic.when(HazelcastUtil::getInstance).thenReturn(hazelcastInstance);
			CustomConfig customConfig = new CustomConfig();
			customConfig.setJsEngineName(TapRunScriptEngine.EngineType.GRAALVM_JS.name());
			count = 123;
			customConfig.setHistoryScript("for(let i = 0;i<" + count + ";i++){\n" +
					"  let id = i+1\n" +
					"  core.push({\"id\": id, \"title\": \"xxxx\", \"created\": new Date()})  \n" +
					"}");
			ReflectionTestUtils.setField(customConnector, "customConfig", customConfig);
		}

		@AfterEach
		void tearDown() {
			Optional.ofNullable(hazelcastUtilMockedStatic).ifPresent(MockedStatic::close);
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