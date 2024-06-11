package io.tapdata.connector.tidb.cdc.process.analyse;

import io.tapdata.entity.error.CoreException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NormalFileReaderTest {
    NormalFileReader reader;
    File file;
    BooleanSupplier alive;

    @BeforeEach
    void init() {
        alive = () -> true;
        reader = new NormalFileReader();
        file = mock(File.class);
    }

    @Test
    void testNormal() throws IOException {
        try(MockedConstruction<FileInputStream> fis = Mockito.mockConstruction(FileInputStream.class);
            MockedConstruction<InputStreamReader>  isr = Mockito.mockConstruction(InputStreamReader.class);
            MockedConstruction<BufferedReader> br = Mockito.mockConstruction(BufferedReader.class, (m, c) -> {
                when(m.readLine()).thenReturn("l1", "", "l2", null);
            })) {
            Assertions.assertDoesNotThrow(() -> reader.readAll(file, alive));
        }
    }

    @Test
    void testNotAlive() throws IOException {
        alive = () -> false;
        try(MockedConstruction<FileInputStream> fis = Mockito.mockConstruction(FileInputStream.class);
            MockedConstruction<InputStreamReader>  isr = Mockito.mockConstruction(InputStreamReader.class);
            MockedConstruction<BufferedReader> br = Mockito.mockConstruction(BufferedReader.class, (m, c) -> {
                when(m.readLine()).thenReturn("l1", "", "l2", null);
            })) {
            Assertions.assertDoesNotThrow(() -> reader.readAll(file, alive));
        }
    }

    @Test
    void testException() throws IOException {
        ReadConsumer<String> consumer = mock(ReadConsumer.class);
        doAnswer(a -> {
            throw new IOException("e");
        }).when(consumer).accept(anyString());
        file = mock(File.class);
        try(MockedConstruction<FileInputStream> fis = Mockito.mockConstruction(FileInputStream.class);
            MockedConstruction<InputStreamReader>  isr = Mockito.mockConstruction(InputStreamReader.class);
            MockedConstruction<BufferedReader> br = Mockito.mockConstruction(BufferedReader.class, (m, c) -> {
                when(m.readLine()).thenReturn("l1", "", "l2", null);
            })) {
            Assertions.assertThrows(CoreException.class, () -> reader.readLineByLine(file, consumer, alive));
        }

    }
}