package io.tapdata.mongodb.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for MapDiffUtil.compareAndFindMissingKeys method
 */
public class MapDiffUtilTest {

    @Test
    @DisplayName("Test empty Map scenarios")
    public void testEmptyMaps() {
        // Test when before is null
        List<MapDiffUtil.KeyDiffInfo> result1 = MapDiffUtil.compareAndFindMissingKeys(null, createMap("key1", "value1"));
        assertTrue(result1.isEmpty(), "Should return empty list when before is null");

        // Test when before is empty Map
        List<MapDiffUtil.KeyDiffInfo> result2 = MapDiffUtil.compareAndFindMissingKeys(new HashMap<>(), createMap("key1", "value1"));
        assertTrue(result2.isEmpty(), "Should return empty list when before is empty Map");

        // Test when after is null
        Map<String, Object> before = createMap("key1", "value1");
        List<MapDiffUtil.KeyDiffInfo> result3 = MapDiffUtil.compareAndFindMissingKeys(before, null);
        assertEquals(1, result3.size(), "Should return all keys in before when after is null");
        assertEquals("key1", result3.get(0).getKey());

        // Test when after is empty Map
        List<MapDiffUtil.KeyDiffInfo> result4 = MapDiffUtil.compareAndFindMissingKeys(before, new HashMap<>());
        assertEquals(1, result4.size(), "Should return all keys in before when after is empty Map");
        assertEquals("key1", result4.get(0).getKey());
    }

    @Test
    @DisplayName("Test basic key differences")
    public void testBasicKeyDifferences() {
        Map<String, Object> before = new HashMap<>();
        before.put("key1", "value1");
        before.put("key2", "value2");
        before.put("key3", "value3");

        Map<String, Object> after = new HashMap<>();
        after.put("key1", "value1");
        after.put("key3", "value3_modified");

        List<MapDiffUtil.KeyDiffInfo> result = MapDiffUtil.compareAndFindMissingKeys(before, after);

        assertEquals(1, result.size(), "Should find 1 missing key");
        assertEquals("key2", result.get(0).getKey());
        assertEquals("key2", result.get(0).getFullPath());
        assertNull(result.get(0).getPathTypes(), "Path types should be null");
    }

    @Test
    @DisplayName("Test nested Map differences")
    public void testNestedMapDifferences() {
        Map<String, Object> before = new HashMap<>();
        Map<String, Object> nestedBefore = new HashMap<>();
        nestedBefore.put("nestedKey1", "nestedValue1");
        nestedBefore.put("nestedKey2", "nestedValue2");
        before.put("nested", nestedBefore);
        before.put("topLevel", "topValue");

        Map<String, Object> after = new HashMap<>();
        Map<String, Object> nestedAfter = new HashMap<>();
        nestedAfter.put("nestedKey1", "nestedValue1");
        after.put("nested", nestedAfter);
        // topLevel does not exist in after

        List<MapDiffUtil.KeyDiffInfo> result = MapDiffUtil.compareAndFindMissingKeys(before, after);

        assertEquals(2, result.size(), "Should find 2 missing keys");

        // Verify result contains expected differences
        Set<String> missingKeys = new HashSet<>();
        Set<String> missingPaths = new HashSet<>();
        for (MapDiffUtil.KeyDiffInfo diffInfo : result) {
            missingKeys.add(diffInfo.getKey());
            missingPaths.add(diffInfo.getFullPath());
            assertNull(diffInfo.getPathTypes(), "Path types should be null");
        }

        assertTrue(missingKeys.contains("topLevel"), "Should contain topLevel");
        assertTrue(missingKeys.contains("nestedKey2"), "Should contain nestedKey2");
        assertTrue(missingPaths.contains("topLevel"), "Should contain topLevel path");
        assertTrue(missingPaths.contains("nested.nestedKey2"), "Should contain nested.nestedKey2 path");
    }

    @Test
    @DisplayName("Test deep nested Map differences")
    public void testDeepNestedMapDifferences() {
        Map<String, Object> before = new HashMap<>();
        Map<String, Object> level1 = new HashMap<>();
        Map<String, Object> level2 = new HashMap<>();
        Map<String, Object> level3 = new HashMap<>();

        level3.put("deepKey", "deepValue");
        level2.put("level3", level3);
        level1.put("level2", level2);
        before.put("level1", level1);

        Map<String, Object> after = new HashMap<>();
        Map<String, Object> afterLevel1 = new HashMap<>();
        Map<String, Object> afterLevel2 = new HashMap<>();
        // level3 does not exist in after
        afterLevel1.put("level2", afterLevel2);
        after.put("level1", afterLevel1);

        List<MapDiffUtil.KeyDiffInfo> result = MapDiffUtil.compareAndFindMissingKeys(before, after);

        assertEquals(1, result.size(), "Should find 1 missing key");
        assertEquals("level3", result.get(0).getKey());
        assertEquals("level1.level2.level3", result.get(0).getFullPath());
        assertNull(result.get(0).getPathTypes(), "Path types should be null");
    }

    @Test
    @DisplayName("Test arrays are ignored")
    public void testArraysAreIgnored() {
        Map<String, Object> before = new HashMap<>();
        before.put("arrayField", Arrays.asList("item1", "item2", "item3"));
        before.put("mapField", createMap("mapKey", "mapValue"));

        Map<String, Object> after = new HashMap<>();
        after.put("arrayField", Arrays.asList("item1")); // Array content is different, but should be ignored
        // mapField does not exist in after

        List<MapDiffUtil.KeyDiffInfo> result = MapDiffUtil.compareAndFindMissingKeys(before, after);

        assertEquals(1, result.size(), "Should only find 1 missing key (array differences ignored)");
        assertEquals("mapField", result.get(0).getKey());
        assertEquals("mapField", result.get(0).getFullPath());
        assertNull(result.get(0).getPathTypes(), "Path types should be null");
    }

    @Test
    @DisplayName("Test Map type mismatch")
    public void testMapTypeMismatch() {
        Map<String, Object> before = new HashMap<>();
        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("innerKey1", "innerValue1");
        nestedMap.put("innerKey2", "innerValue2");
        before.put("field", nestedMap);

        Map<String, Object> after = new HashMap<>();
        after.put("field", "stringValue"); // before is Map, after is String

        List<MapDiffUtil.KeyDiffInfo> result = MapDiffUtil.compareAndFindMissingKeys(before, after);

        assertEquals(2, result.size(), "Should find 2 missing keys");

        Set<String> missingKeys = new HashSet<>();
        Set<String> missingPaths = new HashSet<>();
        for (MapDiffUtil.KeyDiffInfo diffInfo : result) {
            missingKeys.add(diffInfo.getKey());
            missingPaths.add(diffInfo.getFullPath());
            assertNull(diffInfo.getPathTypes(), "Path types should be null");
        }

        assertTrue(missingKeys.contains("innerKey1"), "Should contain innerKey1");
        assertTrue(missingKeys.contains("innerKey2"), "Should contain innerKey2");
        assertTrue(missingPaths.contains("field.innerKey1"), "Should contain field.innerKey1 path");
        assertTrue(missingPaths.contains("field.innerKey2"), "Should contain field.innerKey2 path");
    }

    @Test
    @DisplayName("Test identical Maps")
    public void testIdenticalMaps() {
        Map<String, Object> map1 = new HashMap<>();
        map1.put("key1", "value1");
        map1.put("key2", createMap("nestedKey", "nestedValue"));

        Map<String, Object> map2 = new HashMap<>();
        map2.put("key1", "value1");
        map2.put("key2", createMap("nestedKey", "nestedValue"));

        List<MapDiffUtil.KeyDiffInfo> result = MapDiffUtil.compareAndFindMissingKeys(map1, map2);

        assertTrue(result.isEmpty(), "Identical Maps should have no differences");
    }

    @Test
    @DisplayName("Test complex mixed scenario")
    public void testComplexMixedScenario() {
        Map<String, Object> before = new HashMap<>();

        // Add various types of data
        before.put("simpleString", "value");
        before.put("simpleNumber", 123);
        before.put("arrayField", Arrays.asList("a", "b", "c"));

        Map<String, Object> nestedMap = new HashMap<>();
        nestedMap.put("nestedString", "nestedValue");
        nestedMap.put("nestedArray", Arrays.asList(1, 2, 3));
        nestedMap.put("deepNested", createMap("deepKey", "deepValue"));
        before.put("nestedMap", nestedMap);

        Map<String, Object> after = new HashMap<>();
        after.put("simpleString", "value");
        after.put("simpleNumber", 456); // Value is different but key exists
        after.put("arrayField", Arrays.asList("a")); // Array content is different, should be ignored

        Map<String, Object> afterNestedMap = new HashMap<>();
        afterNestedMap.put("nestedString", "nestedValue");
        afterNestedMap.put("nestedArray", Arrays.asList(1, 2)); // Array differences should be ignored
        // deepNested does not exist in after
        after.put("nestedMap", afterNestedMap);

        List<MapDiffUtil.KeyDiffInfo> result = MapDiffUtil.compareAndFindMissingKeys(before, after);

        assertEquals(1, result.size(), "Should only find 1 missing key");
        assertEquals("deepNested", result.get(0).getKey());
        assertEquals("nestedMap.deepNested", result.get(0).getFullPath());
        assertNull(result.get(0).getPathTypes(), "Path types should be null");
    }

    /**
     * Helper method: Create a simple Map
     */
    private Map<String, Object> createMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }
}
