package io.tapdata.mongodb.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author samuel
 * @Description
 * @create 2024-04-08 10:53
 **/
@DisplayName("Class io.tapdata.mongodb.util.MapUtil Test")
public class MapUtilTest {
	@Nested
	@DisplayName("Method recursiveFlatMap Test")
	class recursiveFlatMapTest {
		@Test
		@DisplayName("test main process")
		void testMainProcess() {
			Map<String, Object> map = new HashMap<>();
			map.put("emptyMap", new HashMap<>());
			map.put("subMap", new HashMap<String, Object>() {{
				put("emptyMap", new HashMap<>());
				put("list", new ArrayList<Object>() {{
					add("1");
					add(new HashMap<>());
				}});
			}});
			map.put("list", new ArrayList<Object>() {{
				add(new HashMap<>());
			}});
			Map<String, Object> newMap = new HashMap<>();
			MapUtil.recursiveFlatMap(map, newMap, "");
			assertEquals(4, newMap.size());
			assertEquals(map.get("emptyMap"), newMap.get("emptyMap"));
			assertEquals(((Map) map.get("subMap")).get("emptyMap"), newMap.get("subMap.emptyMap"));
			assertEquals(((Map) map.get("subMap")).get("list"), newMap.get("subMap.list"));
			assertEquals(map.get("list"), newMap.get("list"));
		}

		@Test
		@DisplayName("test input null")
		void testInputNull() {
			assertDoesNotThrow(() -> MapUtil.recursiveFlatMap(null, new HashMap<>(), ""));
			Map<String, Object> map = new HashMap<>();
			map.put("test", 1);
			assertDoesNotThrow(() -> MapUtil.recursiveFlatMap(map, null, ""));
			Map<String, Object> newMap = new HashMap<>();
			assertDoesNotThrow(() -> MapUtil.recursiveFlatMap(map, newMap, null));
		}
	}
}
