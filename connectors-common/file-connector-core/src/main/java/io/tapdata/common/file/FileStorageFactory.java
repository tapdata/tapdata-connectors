package io.tapdata.common.file;

import io.tapdata.common.FileProtocolEnum;
import io.tapdata.file.TapFileStorage;
import io.tapdata.file.TapFileStorageBuilder;
import io.tapdata.file.operation.FileEndpoint;
import io.tapdata.file.operation.FileOperationErrorCode;
import io.tapdata.file.operation.FileOperationException;

import java.util.Map;

public final class FileStorageFactory {
    private FileStorageFactory() {
    }

    public static TapFileStorage build(FileEndpoint endpoint) throws Exception {
        return build(endpoint.getProtocol(), new DefaultFileServiceConfigMapper().mapStorageParams(endpoint));
    }

    public static TapFileStorage build(String protocolName, Map<String, Object> params) throws Exception {
        FileProtocolEnum protocol = FileProtocolEnum.fromValue(protocolName);
        if (protocol == FileProtocolEnum.UNSUPPORTED || protocol.getStorage() == null) {
            throw new FileOperationException(FileOperationErrorCode.FILE_CONFIG_INVALID,
                    "unsupported file protocol: " + protocolName);
        }
        String className = protocol.getStorage();
        Class<?> storageClass = Class.forName(className);
        return new TapFileStorageBuilder()
                .withClassLoader(storageClass.getClassLoader())
                .withParams(params)
                .withStorageClassName(className)
                .build();
    }
}
