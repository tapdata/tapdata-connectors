package io.tapdata.common.file;

import io.tapdata.file.operation.FileOperationErrorCode;
import io.tapdata.file.operation.FileOperationException;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileStorageFactoryTest {
    @Test
    void rejectsUnsupportedProtocolBeforeBuildingStorage() {
        FileOperationException exception = assertThrows(FileOperationException.class,
                () -> FileStorageFactory.build("unsupported", Collections.emptyMap()));
        assertEquals(FileOperationErrorCode.FILE_CONFIG_INVALID, exception.getCode());
    }
}
