package io.tapdata.bigquery.service.bigQuery;

import com.google.auth.oauth2.ServiceAccountCredentials;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

public class SqlMarkerTest {
    private SqlMarker sqlMarker;
    @BeforeEach
    void beforeEach(){
        sqlMarker = mock(SqlMarker.class);
    }
    @Nested
    class TestConstructorWithEx{
        @Test
        void testWithStringParam(){
            try (MockedStatic<ServiceAccountCredentials> mb = Mockito
                    .mockStatic(ServiceAccountCredentials.class)) {
                mb.when(()->ServiceAccountCredentials.fromStream(any(ByteArrayInputStream.class))).thenThrow(IOException.class);
                String credentialsJson = "credentialsJson";
                assertThrows(RuntimeException.class,()->SqlMarker.create(credentialsJson));
            }
        }
    }
}
