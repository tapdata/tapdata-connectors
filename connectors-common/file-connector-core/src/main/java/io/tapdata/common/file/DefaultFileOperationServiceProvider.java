package io.tapdata.common.file;

import io.tapdata.file.operation.FileOperationServiceProvider;
import io.tapdata.file.operation.TapFileOperationService;

/** ServiceLoader entry point used by engine code without depending on connector classes. */
public final class DefaultFileOperationServiceProvider implements FileOperationServiceProvider {
    private final TapFileOperationService service = new DefaultFileOperationService();

    @Override
    public TapFileOperationService getService() {
        return service;
    }
}
