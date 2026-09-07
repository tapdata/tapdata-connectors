package io.tapdata.common.file;

import io.tapdata.file.operation.FileEndpoint;

import java.util.Map;

public interface FileServiceConfigMapper {
    Map<String, Object> mapStorageParams(FileEndpoint endpoint);
}
