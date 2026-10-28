package io.tapdata.connector.csv;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CustomCsvParser;
import io.tapdata.connector.csv.config.CsvConfig;
import io.tapdata.connector.csv.writer.DelimitedCsvWriter;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CustomCsvDelimiterTest {
    @Test
    void readsMultiCharacterDelimiterAndQuotedDelimiterContent() throws Exception {
        try (CSVReader reader = new CSVReaderBuilder(new StringReader("name||note\nAda||\"likes || tea\""))
                .withCSVParser(new CustomCsvParser("||")).build()) {
            assertArrayEquals(new String[]{"name", "note"}, reader.readNext());
            assertArrayEquals(new String[]{"Ada", "likes || tea"}, reader.readNext());
        }
    }

    @Test
    void preservesLegacyBackslashQuoteEscaping() throws Exception {
        try (CSVReader reader = new CSVReaderBuilder(new StringReader("\"said \\\"hello\\\"\""))
                .withCSVParser(new CustomCsvParser(",")).build()) {
            assertArrayEquals(new String[]{"said \"hello\""}, reader.readNext());
        }
    }

    @Test
    void writesFieldsContainingDelimiterQuotesAndNewlines() throws Exception {
        StringWriter output = new StringWriter();
        DelimitedCsvWriter writer = new DelimitedCsvWriter(output, "||", '"', "\n");
        writer.writeNext(new String[]{"Ada", "tea || coffee", "said \"hello\"", "two\nlines"});

        try (CSVReader reader = new CSVReaderBuilder(new StringReader(output.toString()))
                .withCSVParser(new CustomCsvParser("||")).build()) {
            assertArrayEquals(new String[]{"Ada", "tea || coffee", "said \"hello\"", "two\nlines"}, reader.readNext());
        }
    }

    @Test
    void keepsCommaAsDefaultAndMapsEscapedTabInCustomMode() {
        assertEquals(",", new CsvConfig().getSeparator());
        Map<String, Object> map = new HashMap<>();
        map.put("separatorType", "custom");
        map.put("separator", "\\t");
        assertEquals("\t", new CsvConfig().load(map).getSeparator());
    }

    @Test
    void rejectsInvalidSeparators() {
        assertThrows(IllegalArgumentException.class, () -> CsvConfig.validateSeparator(""));
        assertThrows(IllegalArgumentException.class, () -> CsvConfig.validateSeparator("123456"));
        assertThrows(IllegalArgumentException.class, () -> CsvConfig.validateSeparator("||\n"));
    }
}
