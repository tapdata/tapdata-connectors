package io.tapdata.connector.csv;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.test.util.ReflectionTestUtils;
import java.io.BufferedReader;
import java.io.StringReader;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class CsvHeaderSkipTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void skippingHeaderLeavesFirstDataRowAvailable(boolean schema) throws Exception {
        try (BufferedReader reader = new BufferedReader(new StringReader("header\nrow\n"))) {
            ReflectionTestUtils.invokeMethod(subject(schema), "skipRead", reader, 1);
            assertEquals("row", reader.readLine());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void skippingBeyondEofStopsReadingAtEof(boolean schema) throws Exception {
        try (BufferedReader reader = spy(new BufferedReader(new StringReader("header\nrow\n")))) {
            ReflectionTestUtils.invokeMethod(subject(schema), "skipRead", reader, 5);
            verify(reader, times(3)).readLine();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void zeroSkipRetainsFirstLine(boolean schema) throws Exception {
        try (BufferedReader reader = new BufferedReader(new StringReader("header\nrow\n"))) {
            ReflectionTestUtils.invokeMethod(subject(schema), "skipRead", reader, 0);
            assertEquals("header", reader.readLine());
        }
    }

    private Object subject(boolean schema) {
        return schema ? mock(OffStandardCsvSchema.class, CALLS_REAL_METHODS)
                : mock(CsvConnector.class, CALLS_REAL_METHODS);
    }
}
