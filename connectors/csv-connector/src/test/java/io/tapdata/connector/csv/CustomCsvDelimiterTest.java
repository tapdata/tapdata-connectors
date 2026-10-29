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
    void ignoresOnlyWhitespaceBeforeAnOpeningQuote() throws Exception {
        for (String separator : new String[]{",", "||"}) {
            CustomCsvParser parser = new CustomCsvParser(separator);
            assertArrayEquals(new String[]{"1", "Alice" + separator + " Inc.", "35"},
                    parser.parseLine("1" + separator + "  \"Alice" + separator + " Inc.\"" + separator + "35"));
            assertArrayEquals(new String[]{"1", "  ", "  Alice  "},
                    parser.parseLine("1" + separator + "  " + separator + "  Alice  "));
            assertArrayEquals(new String[]{"  Alice  ", "35"},
                    parser.parseLine("  \"  Alice  \"" + separator + "35"));
        }
    }

    @Test
    void roundTripsDelimiterPrefixesAtFieldBoundaries() throws Exception {
        for (String separator : new String[]{"||", "|||", "aba", "ababa", "abcde"}) {
            for (int length = 1; length < separator.length(); length++) {
                String[] values = {"a" + separator.substring(0, length), "b", ""};
                assertRoundTrip(values, separator, '"');
            }
        }
    }

    @Test
    void roundTripsBackslashesInsideQuotedFields() throws Exception {
        for (char quote : new char[]{'"', '\''}) {
            for (String separator : new String[]{",", "||"}) {
                String[] values = {"two\\\\slashes" + separator, "trailing" + separator + "\\",
                        "slash\\" + quote + "quote", "unquoted\\\\slashes", "two\nlines\\\\"};
                assertRoundTrip(values, separator, quote);
                CustomCsvParser parser = new CustomCsvParser(separator, quote);
                assertArrayEquals(values, parser.parseLine(parser.parseToLine(values, true)));
            }
        }
    }

    private void assertRoundTrip(String[] values, String separator, char quote) throws Exception {
        StringWriter output = new StringWriter();
        DelimitedCsvWriter writer = new DelimitedCsvWriter(output, separator, quote, "\n");
        writer.writeNext(values);
        try (CSVReader reader = new CSVReaderBuilder(new StringReader(output.toString()))
                .withCSVParser(new CustomCsvParser(separator, quote)).build()) {
            assertArrayEquals(values, reader.readNext(), output.toString());
        }
        CustomCsvParser parser = new CustomCsvParser(separator, quote);
        assertArrayEquals(values, parser.parseLine(parser.parseToLine(values, false)));
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
