package io.tapdata.connector.csv.writer;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CustomCsvParser;
import io.tapdata.connector.csv.config.CsvConfig;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CsvCacheMergeTest {
    @TempDir
    Path directory;

    @Test
    void mergesCacheUsingConfiguredQuoteAndDelimiter() throws Exception {
        assertCacheMerge("'", "Ada||'tea || coffee'\n", new String[]{"Ada", "tea || coffee"});
    }

    @Test
    void mergesCacheWithQuotingDisabled() throws Exception {
        assertCacheMerge("", "Ada||literal\"quote\n", new String[]{"Ada", "literal\"quote"});
    }

    private void assertCacheMerge(String quote, String content, String[] expected) throws Exception {
        CsvConfig config = new CsvConfig();
        config.setSeparator("||");
        config.setFileEncoding("UTF-8");
        config.setQuoteChar(quote);
        TapFileStorage storage = mock(TapFileStorage.class);
        TapFile cache = mock(TapFile.class);
        when(cache.getPath()).thenReturn("cache.csv");
        doAnswer(call -> {
            Consumer<InputStream> consumer = call.getArgument(1);
            consumer.accept(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));
            return null;
        }).when(storage).readFile(eq("cache.csv"), any());

        Path output = directory.resolve("merged.csv");
        CacheWriter writer = new CacheWriter(storage, config);
        writer.writeCacheFile(output.toString(), Collections.singletonList(cache));
        char quoteChar = quote.isEmpty() ? '\0' : quote.charAt(0);
        try (CSVReader reader = new CSVReaderBuilder(Files.newBufferedReader(output, StandardCharsets.UTF_8))
                .withCSVParser(new CustomCsvParser("||", quoteChar)).build()) {
            assertArrayEquals(expected, reader.readNext());
            assertNull(reader.readNext());
        }
    }

    private static class CacheWriter extends AbstractCsvRecordWriter {
        CacheWriter(TapFileStorage storage, CsvConfig config) throws Exception {
            super(storage, config, new TapTable("csv").add(new TapField("name", "STRING")), mock(KVMap.class));
        }

        @Override
        public void write(List<TapRecordEvent> events, Consumer<WriteListResult<TapRecordEvent>> consumer) {
            throw new UnsupportedOperationException();
        }
    }
}
