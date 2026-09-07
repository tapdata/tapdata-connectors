package io.tapdata.common.file;

import io.tapdata.file.operation.TapFileOperationService;
import org.junit.jupiter.api.Test;

import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.*;

class DefaultFileOperationServiceProviderTest {
    @Test
    void providerExposesOnlySharedApiService() {
        DefaultFileOperationServiceProvider provider = new DefaultFileOperationServiceProvider();
        TapFileOperationService service = provider.getService();
        assertNotNull(service);
        assertTrue(service instanceof DefaultFileOperationService);
        service.close();
    }

    @Test
    void providerIsDiscoverableThroughServiceLoader() {
        assertTrue(ServiceLoader.load(io.tapdata.file.operation.FileOperationServiceProvider.class)
                .stream().anyMatch(provider -> provider.type().equals(DefaultFileOperationServiceProvider.class)));
    }
}
