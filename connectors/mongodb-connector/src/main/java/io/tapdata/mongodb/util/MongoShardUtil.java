package io.tapdata.mongodb.util;

import com.alibaba.fastjson.JSONObject;
import io.tapdata.entity.schema.TapIndexEx;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

public class MongoShardUtil {

    public static void saveCollectionStats(TapTable table, Map<String, Object> attrs, Map<String, Object> sharkedKeys) {
        if (null != attrs && !attrs.isEmpty()) {
            Map<String, Object> map = new HashMap<>();
            map.put("ns", attrs.get("ns"));
            map.put("max", attrs.get("max"));
            map.put("capped", attrs.get("capped"));
            map.put("maxSize", attrs.get("maxSize"));
            map.put("size", attrs.get("size"));
            map.put("storageSize", attrs.get("storageSize"));
            map.put("avgObjSize", attrs.get("avgObjSize"));
            map.put("indexSize", attrs.get("indexSize"));
            Object sharded = attrs.get("sharded");
            Object shards = attrs.get("shards");
            map.put("sharded", sharded);
            map.put("shards", removeChar(shards));
            map.put("shard", removeChar(sharkedKeys));
            if (Boolean.TRUE.equals(sharded)) {
                if (null == sharkedKeys) {
                    sharkedKeys = new HashMap<>();
                }
                if (sharkedKeys.isEmpty()) {
                    sharkedKeys.put("unique", false);
                    Map<String, Object> key = new HashMap<>();
                    key.put("_id", 1);
                    sharkedKeys.put("key", key);
                }
                Object uniqueObj = sharkedKeys.get("unique");
                Object shardKeysObj = sharkedKeys.get("key");
                if (shardKeysObj instanceof Map && !((Map<String, Object>)shardKeysObj).isEmpty()) {
                    Set<String> shardKeys = ((Map<String, Object>) shardKeysObj).keySet();
                    List<TapIndexField> indexFields = new ArrayList<>();
                    shardKeys.stream().filter(Objects::nonNull).forEach(shardKey -> {
                        TapIndexField field = new TapIndexField();
                        field.setName(shardKey);
                        // 1 or hashed
                        String shardKeyType = String.valueOf(((Map<String, Object>) shardKeysObj).get(shardKey));
                        field.setFieldAsc("1".equals(shardKeyType));
                        field.setType(shardKeyType);
                        indexFields.add(field);
                    });
                    TapIndexEx indexEx = new TapIndexEx();
                    indexEx.setIndexFields(indexFields);
                    indexEx.setUnique(uniqueObj instanceof Boolean && ((Boolean)uniqueObj));
                    indexEx.setPrimary(false);
                    indexEx.setName(UUID.randomUUID().toString());
                    indexEx.setCluster(false);
                    map.put("partitionIndex", JSONObject.toJSONString(indexEx));
                    table.setPartitionIndex(indexEx);
                }
            } else {
                table.setPartitionIndex(null);
            }
            table.setTableAttr(map);
        }
    }

    public static Object removeChar(Object collection) {
        if (collection instanceof Map) {
            return removeChar((Map<String, Object>) collection);
        } else if (collection instanceof Collection) {
            return removeColl((Collection<?>) collection);
        }
        return collection;
    }

    public static Object removeColl(Collection<?> collection){
        if (null == collection ) return null;
        ArrayList<Object> list = new ArrayList<>();
        for (Object o : collection) {
            list.add(removeChar(o));
        }
        return list;
    }

    public static Object removeChar(Map<String, Object> collection) {
        Map<String, Object> map = new HashMap<>();
        if (null != collection && !collection.isEmpty()) {
            Set<String> strings = collection.keySet();
            for (String key : strings) {
                if (!key.contains("$")) {
                    map.put(key, removeChar(collection.get(key)));
                }
            }
            return map;
        }
        return null != collection ? map : null;
    }
}
