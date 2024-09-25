package io.tapdata.connector.config;

import io.tapdata.connector.IConfigWithContext;

import java.util.*;
import java.util.function.BiConsumer;

/**
 * 扩展属性配置
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/28 11:53 Create
 */
public interface ConnectionExtParams extends IConfigWithContext {

    String KEY_EXT_PARAMS = "extParams";
    String KEY_EXT_PARAMS_TYPE = "type";
    String KEY_EXT_PARAMS_KEY = "key";
    String KEY_EXT_PARAMS_VAL = "val";

    default List<Map<String, String>> getConnectionExtParams() {
        ArrayList<Map<String, String>> defaultValue = new ArrayList<>();
        return connectionConfigGet(KEY_EXT_PARAMS, defaultValue);
    }

    /**
     * 遍历指定类型的配置
     *
     * @param consumer 配置项回调
     * @param types    指定需要获取的类型
     */
    default void eachConnectionExtParams(BiConsumer<String, String> consumer, String... types) {
        List<Map<String, String>> connectionExtParams = getConnectionExtParams();
        if (null != connectionExtParams && !connectionExtParams.isEmpty()) {
            String type;
            String key;
            String val;
            if (null == types || types.length == 0) {
                for (Map<String, String> connectionExtParam : connectionExtParams) {
                    key = connectionExtParam.get(KEY_EXT_PARAMS_KEY);
                    val = connectionExtParam.get(KEY_EXT_PARAMS_VAL);
                    consumer.accept(key, val);
                }
            } else {
                Set<String> typeSet = new HashSet<>();
                for (String t : types) {
                    typeSet.add(t.toUpperCase());
                }
                for (Map<String, String> connectionExtParam : connectionExtParams) {
                    type = connectionExtParam.get(KEY_EXT_PARAMS_TYPE);
                    if (typeSet.contains(type)) {
                        key = connectionExtParam.get(KEY_EXT_PARAMS_KEY);
                        val = connectionExtParam.get(KEY_EXT_PARAMS_VAL);
                        consumer.accept(key, val);
                    }
                }
            }
        }
    }

}

