package io.tapdata.mongodb;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import io.tapdata.mongodb.entity.MongodbConfig;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.TestItem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.SQLException;
import java.util.function.Consumer;

import static org.mockito.Mockito.*;

public class TestMongodbTest {
    @Nested
    class TestTimeDifference{
        MongodbTest mongodbTest;
        @BeforeEach
        void init(){
            MongodbConfig mongodbConfig = new MongodbConfig();
            mongodbConfig.setUri("mongodb://123.123.1.123:12345/test");
            Consumer<TestItem> consumer = testItem -> {
            };
            mongodbTest = new MongodbTest(mongodbConfig,consumer,mock(MongoClient.class),new ConnectionOptions());
        }
        @Test
        void testPass(){
            try(MockedStatic<MongodbUtil> mockedStatic = mockStatic(MongodbUtil.class)){
                mockedStatic.when(() -> MongodbUtil.getServerTime(any(), any())).thenReturn(System.currentTimeMillis());
                mongodbTest.testTimeDifference();
                ConnectionOptions connectionOptions = (ConnectionOptions) ReflectionTestUtils.getField(mongodbTest,"connectionOptions");
                Assertions.assertEquals(0,connectionOptions.getTimeDifference());
            }
        }
        @Test
        void testFail(){
            try(MockedStatic<MongodbUtil> mockedStatic = mockStatic(MongodbUtil.class)){
                mockedStatic.when(() -> MongodbUtil.getServerTime(any(), any())).thenReturn(System.currentTimeMillis() + 2000);
                mongodbTest.testTimeDifference();
                ConnectionOptions connectionOptions = (ConnectionOptions) ReflectionTestUtils.getField(mongodbTest,"connectionOptions");
                Assertions.assertTrue(connectionOptions.getTimeDifference() > 1000);
            }
        }

        @Test
        void testQuerySourceTimeFail(){
            try(MockedStatic<MongodbUtil> mockedStatic = mockStatic(MongodbUtil.class)){
                mockedStatic.when(() -> MongodbUtil.getServerTime(any(), any())).thenThrow(new MongoException("test"));
                mongodbTest.testTimeDifference();
                ConnectionOptions connectionOptions = (ConnectionOptions) ReflectionTestUtils.getField(mongodbTest,"connectionOptions");
                Assertions.assertNull(connectionOptions.getTimeDifference());
            }
        }

    }
}
