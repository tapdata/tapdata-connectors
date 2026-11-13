package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.write.PaimonTableWriteContext;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PaimonRuntimeDirectoryEvidenceTest {
    @Test
    @SuppressWarnings("unchecked")
    void runtimeCommitFailureReportsDeletedSpillAndBufferStateWithoutReplacingCause(@TempDir Path root)
            throws Exception {
        Path spill = Files.createDirectory(root.resolve("spill"));
        Path buffer = Files.createDirectory(root.resolve("buffer"));
        PaimonConfig config = new PaimonConfig();
        config.setDatabase("default");
        config.setEnableAsyncCommit(false);
        config.setStorageType("s3");
        config.setWarehouse("s3a://evidence-bucket/");
        config.setDiskTmpDir(root.toString());
        config.setS3Endpoint("http://127.0.0.1:9");
        Path unusedGlobal = Files.write(root.resolve("unused-global-file"), new byte[0]);
        config.setS3Properties(java.util.Arrays.asList(
                property("fs.s3a.buffer.dir", unusedGlobal.toString()),
                property("fs.s3a.bucket.evidence-bucket.buffer.dir", buffer.toString())));
        Log log = mock(Log.class);
        PaimonService service = new PaimonService(config, log);
        java.lang.reflect.Method build = PaimonService.class.getDeclaredMethod("buildHadoopConfiguration");
        build.setAccessible(true);
        build.invoke(service);
        service.startForTest();
        PaimonTableWriteContext context = mock(PaimonTableWriteContext.class);
        when(context.spillDirs()).thenReturn(Collections.singletonList(spill.toString()));
        IOException original = new IOException("Channel to path could not be opened",
                new java.io.FileNotFoundException(spill.resolve("channel").toString()));
        when(context.commit()).thenThrow(original);
        Field contexts = field("tableWriteContexts");
        ((Map<String, PaimonTableWriteContext>) contexts.get(service)).put("default.orders", context);
        Files.delete(spill);
        TapTable table = mock(TapTable.class);
        when(table.getName()).thenReturn("orders");
        TapConnectorContext connector = mock(TapConnectorContext.class);
        when(connector.getStateMap()).thenReturn(mock(io.tapdata.entity.utils.cache.KVMap.class));
        try {
            assertSame(original, assertThrows(IOException.class,
                    () -> service.afterInitialSync(connector, table)));
            verify(log, timeout(5000)).error(argThat(message -> message.contains("stage=write-failed")
                    && message.contains(spill.toString()) && message.contains("exists=false")
                    && message.contains(buffer.toString()) && message.contains("fileStoreType=")
                    && message.contains("readOnly=") && message.contains("usableBytes=")
                    && !message.contains(unusedGlobal.toString())
                    && message.contains("FileNotFoundException")));
            assertTrue(original.getCause() instanceof java.io.FileNotFoundException);
        } finally {
            try { service.close(); } catch (Exception expectedStickyFailure) { }
        }
    }

    private static Field field(String name) throws Exception {
        Field field = PaimonService.class.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }

    private static LinkedHashMap<String, String> property(String key, String value) {
        LinkedHashMap<String, String> property = new LinkedHashMap<>();
        property.put("propKey", key);
        property.put("propValue", value);
        return property;
    }
}
