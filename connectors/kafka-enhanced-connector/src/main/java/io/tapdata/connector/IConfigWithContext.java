package io.tapdata.connector;

import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 连接器配置接口
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/8/28 14:17 Create
 */
public interface IConfigWithContext {

    static <T> void acceptNotNull(T param, Consumer<T> fn) {
        if (null != param) {
            fn.accept(param);
        }
    }

    static <P, R> R applyNotNull(P param, Function<P, R> fn, R def) {
        if (null == param) {
            return def;
        }

        R result = fn.apply(param);
        if (null == result) {
            return def;
        }
        return result;
    }

    TapConnectionContext tapConnectionContext();

    default void tapConnectionContext(Consumer<TapConnectionContext> callback) {
        acceptNotNull(tapConnectionContext(), callback);
    }

    default <T> T tapConnectionContext(Function<TapConnectionContext, T> fn, T def) {
        return applyNotNull(tapConnectionContext(), fn, def);
    }

    default TapConnectorContext tapConnectorContext() {
        return applyNotNull(tapConnectionContext(), context -> {
            if (context instanceof TapConnectorContext) {
                return (TapConnectorContext) context;
            }
            return null;
        }, null);
    }

    default void tapConnectorContext(Consumer<TapConnectorContext> callback) {
        acceptNotNull(tapConnectorContext(), callback);
    }

    default <T> T tapConnectorContext(Function<TapConnectorContext, T> fn, T def) {
        return applyNotNull(tapConnectorContext(), fn, def);
    }

    default DataMap connectionConfig() {
        return tapConnectionContext(TapConnectionContext::getConnectionConfig, null);
    }

    default <T> T connectionConfigGet(String key, T def) {
        DataMap dataMap = connectionConfig();
        if (null == dataMap) {
            return def;
        }
        return dataMap.getValue(key, def);
    }

    default DataMap nodeConfig() {
        return tapConnectionContext(TapConnectionContext::getNodeConfig, null);
    }

    default void nodeConfig(Consumer<DataMap> consumer) {
        acceptNotNull(nodeConfig(), consumer);
    }

    default <T> T nodeConfig(Function<DataMap, T> fn, T def) {
        return applyNotNull(nodeConfig(), fn, def);
    }

    default <T> T nodeConfigGet(String key, T def) {
        DataMap dataMap = nodeConfig();
        if (null == dataMap) {
            return def;
        }
        return dataMap.getValue(key, def);
    }

    default KVReadOnlyMap<TapTable> tableMap() {
        return tapConnectorContext(TapConnectorContext::getTableMap, null);
    }

    default <T> T tableMap(Function<KVReadOnlyMap<TapTable>, T> fn, T def) {
        return applyNotNull(tableMap(), fn, def);
    }

    default TapTable tableMapGet(String key) {
        return tableMapGet(key, null);
    }

    default TapTable tableMapGet(String key, TapTable def) {
        return applyNotNull(tableMap(), tableMap -> tableMap.get(key), def);
    }

    default KVMap<Object> stateMap() {
        return tapConnectorContext(TapConnectorContext::getStateMap, null);
    }

    default boolean stateMapSet(String key, Object val) {
        KVMap<Object> map = stateMap();
        if (null != map) {
            map.put(key, val);
            return true;
        }
        return false;
    }

    default Object stateMapGet(String key) {
        return stateMapGet(key, null);
    }

    default Object stateMapGet(String key, Object def) {
        return applyNotNull(stateMap(), stateMap -> stateMap.get(key), def);
    }

    default String getStateMapFirstConnectorId() {
        return (String) stateMapGet("firstConnectorId");
    }
}
