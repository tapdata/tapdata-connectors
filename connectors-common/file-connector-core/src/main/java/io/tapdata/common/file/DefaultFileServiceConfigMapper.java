package io.tapdata.common.file;

import io.tapdata.file.operation.FileEndpoint;

import java.util.LinkedHashMap;
import java.util.Map;

public class DefaultFileServiceConfigMapper implements FileServiceConfigMapper {
    @Override
    public Map<String, Object> mapStorageParams(FileEndpoint endpoint) {
        if (endpoint == null) throw new IllegalArgumentException("endpoint is required");
        Map<String, Object> source = endpoint.getParams();
        Map<String, Object> result = new LinkedHashMap<>(source);
        String protocol = endpoint.getProtocol();
        result.putIfAbsent("protocol", protocol);
        copyAlias(result, "host", protocol + "Host");
        copyAlias(result, "port", protocol + "Port");
        copyAlias(result, "username", protocol + "Username");
        copyAlias(result, "password", protocol + "Password");
        copyAlias(result, "ssl", protocol + "Ssl");
        copyAlias(result, "passiveMode", protocol + "PassiveMode");
        copyAlias(result, "connectTimeout", protocol + "ConnectTimeout");
        copyAlias(result, "dataTimeout", protocol + "DataTimeout");
        return result;
    }

    private void copyAlias(Map<String, Object> params, String from, String to) {
        if (params.containsKey(from) && !params.containsKey(to)) {
            params.put(to, params.get(from));
        }
    }
}
