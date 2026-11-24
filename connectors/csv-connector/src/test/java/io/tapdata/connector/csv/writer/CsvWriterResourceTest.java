package io.tapdata.connector.csv.writer;

import io.tapdata.connector.csv.config.CsvConfig;
import io.tapdata.file.TapFileStorage;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CsvWriterResourceTest {
    @Test
    void newWriterOpensOneStreamAndRetainsConfiguredCsvRules() throws Exception {
        TapFileStorage storage = mock(TapFileStorage.class);
        TrackingStream output = new TrackingStream();
        when(storage.openFileOutputStream("records.csv", true)).thenReturn(output);
        CsvConfig config = new CsvConfig();
        config.setFileEncoding("UTF-8");
        AbstractCsvRecordWriter records = records(storage, config);
        CsvFileWriter writer = records.getCsvFileWriterAndInit("records.csv");
        writer.getCsvWriter().writeNext(new String[]{"1"});
        writer.close();

        verify(storage).openFileOutputStream("records.csv", true);
        assertTrue(output.closed);
        assertEquals("id\n1\n", new String(output.toByteArray(), StandardCharsets.UTF_8));
    }

    @Test
    void invalidEncodingClosesStreamEvenWhenConstructionFails() throws Exception {
        TapFileStorage storage = mock(TapFileStorage.class);
        TrackingStream output = new TrackingStream();
        when(storage.openFileOutputStream("records.csv", true)).thenReturn(output);
        CsvConfig config = new CsvConfig();
        config.setFileEncoding("invalid-encoding");
        AbstractCsvRecordWriter records = records(storage, config);

        assertThrows(UnsupportedEncodingException.class,
                () -> records.getCsvFileWriterAndInit("records.csv"));
        assertTrue(output.closed);
    }

    @Test
    void failedRuleConfigurationClosesTheOpenedStream() throws Exception {
        TapFileStorage storage = mock(TapFileStorage.class);
        TrackingStream output = new TrackingStream();
        when(storage.openFileOutputStream("records.csv", true)).thenReturn(output);
        CsvConfig config = spy(new CsvConfig());
        config.setFileEncoding("UTF-8");
        IllegalArgumentException failure = new IllegalArgumentException("invalid CSV rules");
        doThrow(failure).when(config).getSeparator();
        AbstractCsvRecordWriter records = records(storage, config);
        assertSame(failure, assertThrows(IllegalArgumentException.class,
                () -> records.getCsvFileWriterAndInit("records.csv")));
        assertTrue(output.closed);
    }

    private AbstractCsvRecordWriter records(TapFileStorage storage, CsvConfig config) {
        AbstractCsvRecordWriter records = mock(AbstractCsvRecordWriter.class, CALLS_REAL_METHODS);
        ReflectionTestUtils.setField(records, "storage", storage);
        ReflectionTestUtils.setField(records, "fileConfig", config);
        ReflectionTestUtils.setField(records, "fileWriterMap", new HashMap<>());
        ReflectionTestUtils.setField(records, "lastWriteMap", new HashMap<>());
        ReflectionTestUtils.setField(records, "fieldList", Collections.singletonList("id"));
        return records;
    }

    private static class TrackingStream extends ByteArrayOutputStream {
        boolean closed;
        @Override
        public void close() {
            closed = true;
        }
    }
}
