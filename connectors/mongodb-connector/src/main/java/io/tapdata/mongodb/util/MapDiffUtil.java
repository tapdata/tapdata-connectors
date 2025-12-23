package io.tapdata.mongodb.util;

import java.util.*;

/**
 * Map comparison utility class
 * Used to compare two Maps, find keys that exist in 'before' but not in 'after', and record detailed path information
 */
public class MapDiffUtil {

    /**
     * Compare two Maps and return information about keys that exist in 'before' but not in 'after'
     *
     * Functionality Description:
     * 1. Compares two Maps to find keys that exist in 'before' but not in 'after'
     * 2. Supports recursive comparison of nested Map structures
     * 3. Special limitations for List type handling:
     *    - In the main comparison flow (compareMapRecursively method), List type comparison is completely ignored
     *    - Only when the 'before' value is a Map and the 'after' value is not a Map, all keys in 'before' will be recorded
     *    - Structural changes within Lists are not detected or recorded
     *
     * List Limitation Boundaries:
     * - Does not support comparison of List element additions/deletions
     * - Does not support detection of element order changes within Lists
     * - Does not support deep comparison of nested Map/List structures within Lists
     * - When a Map field changes from List type to other types, differences will not be recorded
     * - When a Map field changes from other types to List type, differences will not be recorded
     *
     * Applicable Scenarios:
     * - Primarily used for detecting missing keys in Map structures
     * - Suitable for document structure change detection, but not for array content change detection
     * - Recommended for schema change detection, not for data content change detection
     *
     * @param before Original Map, cannot be null
     * @param after  Target Map, can be null (will be treated as empty Map)
     * @return List of key difference information containing detailed path information of missing keys.
     *         Returns empty list if 'before' is empty
     */
    public static List<KeyDiffInfo> compareAndFindMissingKeys(Map<String, Object> before, Map<String, Object> after) {
        List<KeyDiffInfo> result = new ArrayList<>();

        if (isEmpty(before)) {
            return result;
        }

        if (isEmpty(after)) {
            after = new HashMap<>();
        }
        
        compareMapRecursively(before, after, "", new ArrayList<>(), result);
        
        return result;
    }

    /**
     * Check if Map is empty
     *
     * @param map Map to check
     * @return true if Map is null or empty
     */
    private static boolean isEmpty(Map<String, Object> map) {
        return map == null || map.isEmpty();
    }

    /**
     * Check if string is null or blank
     *
     * @param str String to check
     * @return true if string is null, empty, or contains only whitespace characters
     */
    private static boolean isBlank(String str) {
        return str == null || str.trim().isEmpty();
    }

    /**
     * Recursively compare Maps (ignore array paths, only compare nested structures of Map type)
     *
     * @param beforeMap    Original Map
     * @param afterMap     Target Map
     * @param currentPath  Current path
     * @param pathTypes    List of path types
     * @param result       Result list
     */
    private static void compareMapRecursively(Map<String, Object> beforeMap,
                                            Map<String, Object> afterMap,
                                            String currentPath,
                                            List<PathType> pathTypes,
                                            List<KeyDiffInfo> result) {

        for (Map.Entry<String, Object> entry : beforeMap.entrySet()) {
            String key = entry.getKey();
            Object beforeValue = entry.getValue();

            // Build current full path
            String fullPath = isBlank(currentPath) ? key : currentPath + "." + key;

            // Check if the key exists in after
            if (!afterMap.containsKey(key)) {
                // Key does not exist in after, record difference (without path types)
                if(key.equals("_id"))continue;
                KeyDiffInfo diffInfo = new KeyDiffInfo(key, fullPath, null);
                result.add(diffInfo);
            } else {
                // Key exists in after, continue recursive comparison
                Object afterValue = afterMap.get(key);
                // Only continue recursive comparison when both values are Maps, ignore array types
                if (beforeValue instanceof Map && afterValue instanceof Map) {
                    compareMapRecursively((Map<String, Object>) beforeValue,
                                        (Map<String, Object>) afterValue,
                                        fullPath,
                                        pathTypes,
                                        result);
                } else if (beforeValue instanceof Map && !(afterValue instanceof Map)) {
                    // before is Map but after is not Map, record all keys in before (only handle Map type)
                    recordAllKeysInMapOnly((Map<String, Object>) beforeValue, fullPath, result);
                }
                // Ignore List type comparison
            }
        }
    }

    /**
     * Recursively compare values
     *
     * @param beforeValue     Original value
     * @param afterValue      Target value
     * @param currentPath     Current path
     * @param pathTypes       List of path types
     * @param result          Result list
     */
    @SuppressWarnings("unchecked")
    private static void compareValueRecursively(Object beforeValue, 
                                              Object afterValue, 
                                              String currentPath, 
                                              List<PathType> pathTypes, 
                                              List<KeyDiffInfo> result) {
        
        if (beforeValue instanceof Map && afterValue instanceof Map) {
            // Both are Maps, recursively compare
            compareMapRecursively((Map<String, Object>) beforeValue,
                                (Map<String, Object>) afterValue,
                                currentPath,
                                pathTypes,
                                result);
        } else if (beforeValue instanceof List && afterValue instanceof List) {
            // Both are Lists, compare elements in List
            compareListRecursively((List<Object>) beforeValue,
                                 (List<Object>) afterValue,
                                 currentPath,
                                 pathTypes,
                                 result);
        } else if (beforeValue instanceof Map && !(afterValue instanceof Map)) {
            // before is Map but after is not Map, record all keys in before
            recordAllKeysInMap((Map<String, Object>) beforeValue, currentPath, pathTypes, result);
        } else if (beforeValue instanceof List && !(afterValue instanceof List)) {
            // before is List but after is not List, record all elements in before
            recordAllKeysInList((List<Object>) beforeValue, currentPath, pathTypes, result);
        }
        // Other cases (primitive type comparison) do not need to be handled, as we only care about structural differences
    }

    /**
     * Recursively compare Lists
     *
     * @param beforeList   Original List
     * @param afterList    Target List
     * @param currentPath  Current path
     * @param pathTypes    List of path types
     * @param result       Result list
     */
    @SuppressWarnings("unchecked")
    private static void compareListRecursively(List<Object> beforeList, 
                                             List<Object> afterList, 
                                             String currentPath, 
                                             List<PathType> pathTypes, 
                                             List<KeyDiffInfo> result) {
        
        // Copy path type list and add current level type
        List<PathType> currentPathTypes = new ArrayList<>(pathTypes);
        currentPathTypes.add(PathType.LIST);

        for (int i = 0; i < beforeList.size(); i++) {
            Object beforeItem = beforeList.get(i);
            String indexPath = currentPath + "[" + i + "]";

            if (i >= afterList.size()) {
                // No corresponding index element in after
                if (beforeItem instanceof Map) {
                    recordAllKeysInMap((Map<String, Object>) beforeItem, indexPath, currentPathTypes, result);
                } else if (beforeItem instanceof List) {
                    recordAllKeysInList((List<Object>) beforeItem, indexPath, currentPathTypes, result);
                }
            } else {
                // Corresponding index element exists in after, continue comparison
                Object afterItem = afterList.get(i);
                compareValueRecursively(beforeItem, afterItem, indexPath, currentPathTypes, result);
            }
        }
    }

    /**
     * Record all keys in Map (only handle Map type, ignore arrays)
     *
     * @param map          Map object
     * @param currentPath  Current path
     * @param result       Result list
     */
    @SuppressWarnings("unchecked")
    private static void recordAllKeysInMapOnly(Map<String, Object> map,
                                             String currentPath,
                                             List<KeyDiffInfo> result) {

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            String fullPath = currentPath + "." + key;

            // Record difference (without path types)
            KeyDiffInfo diffInfo = new KeyDiffInfo(key, fullPath, null);
            result.add(diffInfo);

            // Only recursively handle nested structures of Map type, ignore List type
            if (value instanceof Map) {
                recordAllKeysInMapOnly((Map<String, Object>) value, fullPath, result);
            }
            // Ignore List type processing
        }
    }

    /**
     * Record all keys in Map
     *
     * @param map          Map object
     * @param currentPath  Current path
     * @param pathTypes    List of path types
     * @param result       Result list
     */
    @SuppressWarnings("unchecked")
    private static void recordAllKeysInMap(Map<String, Object> map,
                                         String currentPath,
                                         List<PathType> pathTypes,
                                         List<KeyDiffInfo> result) {

        List<PathType> currentPathTypes = new ArrayList<>(pathTypes);
        currentPathTypes.add(PathType.MAP);

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            String fullPath = currentPath + "." + key;

            KeyDiffInfo diffInfo = new KeyDiffInfo(key, fullPath, new ArrayList<>(currentPathTypes));
            result.add(diffInfo);

            // Recursively handle nested structures
            if (value instanceof Map) {
                recordAllKeysInMap((Map<String, Object>) value, fullPath, currentPathTypes, result);
            } else if (value instanceof List) {
                recordAllKeysInList((List<Object>) value, fullPath, currentPathTypes, result);
            }
        }
    }

    /**
     * Record all elements in List
     *
     * @param list         List object
     * @param currentPath  Current path
     * @param pathTypes    List of path types
     * @param result       Result list
     */
    @SuppressWarnings("unchecked")
    private static void recordAllKeysInList(List<Object> list, 
                                          String currentPath, 
                                          List<PathType> pathTypes, 
                                          List<KeyDiffInfo> result) {
        
        List<PathType> currentPathTypes = new ArrayList<>(pathTypes);
        currentPathTypes.add(PathType.LIST);
        
        for (int i = 0; i < list.size(); i++) {
            Object item = list.get(i);
            String indexPath = currentPath + "[" + i + "]";
            
            if (item instanceof Map) {
                recordAllKeysInMap((Map<String, Object>) item, indexPath, currentPathTypes, result);
            } else if (item instanceof List) {
                recordAllKeysInList((List<Object>) item, indexPath, currentPathTypes, result);
            }
        }
    }

    /**
     * Path type enumeration
     */
    public enum PathType {
        MAP("map"),
        LIST("list");
        
        private final String type;
        
        PathType(String type) {
            this.type = type;
        }
        
        public String getType() {
            return type;
        }
        
        @Override
        public String toString() {
            return type;
        }
    }

    /**
     * Key difference information class
     */
    public static class KeyDiffInfo {
        private final String key;
        private final String fullPath;
        private final List<PathType> pathTypes;
        
        public KeyDiffInfo(String key, String fullPath, List<PathType> pathTypes) {
            this.key = key;
            this.fullPath = fullPath;
            this.pathTypes = pathTypes;
        }
        
        public String getKey() {
            return key;
        }
        
        public String getFullPath() {
            return fullPath;
        }
        
        public List<PathType> getPathTypes() {
            return pathTypes;
        }
        
        @Override
        public String toString() {
            return "KeyDiffInfo{" +
                    "key='" + key + '\'' +
                    ", fullPath='" + fullPath + '\'' +
                    ", pathTypes=" + pathTypes +
                    '}';
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            KeyDiffInfo that = (KeyDiffInfo) o;
            return Objects.equals(key, that.key) &&
                    Objects.equals(fullPath, that.fullPath) &&
                    Objects.equals(pathTypes, that.pathTypes);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(key, fullPath, pathTypes);
        }
    }
}
