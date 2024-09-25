package io.tapdata.kafka.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.tapdata.events.EventOperation;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 标准事件工具
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/23 14:48 Create
 */
public class StandardEventUtils {
    /**
     * 操作类型
     */
    public static final String KEY_OP = "op";
    /**
     * 操作时间
     */
    public static final String KEY_OP_TS = "opTs";
    /**
     * 应用解析时间
     */
    public static final String KEY_TS = "ts";
    /**
     * 命名空间
     */
    public static final String KEY_NAMESPACES = "namespaces";
    /**
     * 表名
     */
    public static final String KEY_TABLE = "table";
    /**
     * 未知事件数据
     */
    public static final String KEY_DATA = "data";
    /**
     * 修改前数据
     */
    public static final String KEY_BEFORE = "before";
    /**
     * 修改后数据
     */
    public static final String KEY_AFTER = "after";

    private StandardEventUtils() {
    }

    // ---------- 基础配置 ----------

    public static String getOpStr(JSONObject json) {
        return json.getString(KEY_OP);
    }

    public static EventOperation getOp(JSONObject json) {
        String op = getOpStr(json);
        if (null != op) {
            return EventOperation.fromString(op);
        }
        return null;
    }

    public static void setOp(Map<String, Object> map, EventOperation op) {
        map.put(KEY_OP, op.toString());
    }

    public static Long getOpTs(JSONObject json) {
        return json.getLong(KEY_OP_TS);
    }

    public static void setOpTs(Map<String, Object> map, Long ts) {
        map.put(KEY_OP_TS, ts);
    }

    public static Long getTs(JSONObject json) {
        return json.getLong(KEY_TS);
    }

    public static void setTs(Map<String, Object> map, Long ts) {
        map.put(KEY_TS, ts);
    }

    public static String getTable(JSONObject json) {
        return json.getString(KEY_TABLE);
    }

    public static void setTable(Map<String, Object> map, String table) {
        map.put(KEY_TABLE, table);
    }

    public static List<String> getNamespaces(JSONObject json) {
        return toList(json, KEY_NAMESPACES, String.class);
    }

    public static void setNamespaces(Map<String, Object> map, List<String> namespaces) {
        map.put(KEY_NAMESPACES, namespaces);
    }

    public static Map<String, Object> getAfter(JSONObject json) {
        return toMap(json, KEY_AFTER);
    }

    public static void setAfter(Map<String, Object> map, Map<String, Object> after) {
        map.put(KEY_AFTER, after);
    }

    public static Map<String, Object> getBefore(JSONObject json) {
        return toMap(json, KEY_BEFORE);
    }

    public static void setBefore(Map<String, Object> map, Map<String, Object> before) {
        map.put(KEY_BEFORE, before);
    }

    public static Serializable getData(JSONObject json) {
        return json.getObject(KEY_DATA, Serializable.class);
    }

    public static void setData(Map<String, Object> map, Serializable data) {
        map.put(KEY_DATA, data);
    }

    // ---------- 工具方法 ----------

    private static Map<String, Object> toMap(JSONObject json, String key) {
        return json.getObject(key, LinkedHashMap.class);
    }

    private static <T> List<T> toList(JSONObject json, String key, Class<T> clz) {
        JSONArray jsonArray = json.getJSONArray(key);
        if (null == jsonArray) return null;

        return jsonArray.toJavaList(clz);
    }
}
