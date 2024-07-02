import cn.hutool.http.HttpUtil;
import io.tapdata.connector.doris.DorisConnector;
import io.tapdata.connector.doris.DorisJdbcContext;
import io.tapdata.connector.doris.bean.DorisConfig;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapYearValue;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.eclipse.jetty.client.HttpClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.TimeZone;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.formatTapDateTime;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class DorisTest {

    //    @Test
//    void testUpdateDateCapabilitiesForTapDateTimeWithoutZone() {
//        testUpdateDateCapabilitiesForTapDateTime(false);
//
//    }
//
//    @Test
//    void testUpdateDateCapabilitiesForTapDateTimeWithZone() {
//        testUpdateDateCapabilitiesForTapDateTime(true);
//    }
//
//    public void testUpdateDateCapabilitiesForTapDateTime(boolean isTimeZone) {
//        DorisConnector dorisConnector = new DorisConnector();
//        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
//        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
//        dorisConnector.registerCapabilities(connectorFunctions, codecRegistry);
//        FromTapValueCodec codec = codecRegistry.getCustomFromTapValueCodec(TapDateTimeValue.class);
//        TapDateTimeValue tapDateTimeValue = new TapDateTimeValue();
//        Timestamp timestamp = new Timestamp(1713962329337L);
//        tapDateTimeValue.setValue(new DateTime(timestamp));
//        tapDateTimeValue.setOriginType("timestamp");
//        DateTime dateTime = new DateTime(timestamp);
//        if (isTimeZone) {
//            TimeZone timeZone = TimeZone.getTimeZone(ZoneId.of("+07:00"));
//            ReflectionTestUtils.setField(dorisConnector, "timezone", timeZone);
//            dateTime.setTimeZone(timeZone);
//        }
//        Object actualData = codec.fromTapValue(tapDateTimeValue);
//        Object exceptionData = formatTapDateTime(dateTime, "yyyy-MM-dd HH:mm:ss.SSSSSS");
//        Assertions.assertEquals(exceptionData, actualData);
//
//    }
//
//
//    @Test
//    void testUpdateDateCapabilitiesForTapDateWithoutZone() {
//        testUpdateDateCapabilitiesForTapDate(false);
//    }
//
//    @Test
//    void testUpdateDateCapabilitiesForTapDateWithZone() {
//        testUpdateDateCapabilitiesForTapDate(true);
//    }
//
//
//    public void testUpdateDateCapabilitiesForTapDate(boolean isTimeZone){
//        DorisConnector dorisConnector = new DorisConnector();
//        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
//        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
//        dorisConnector.registerCapabilities(connectorFunctions, codecRegistry);
//        FromTapValueCodec codec = codecRegistry.getCustomFromTapValueCodec(TapDateValue.class);
//        TapDateValue tapDateValue = new TapDateValue();
//        Timestamp timestamp = new Timestamp(1713962329337L);
//        tapDateValue.setValue(new DateTime(timestamp));
//        tapDateValue.setOriginType("timestamp");
//        DateTime dateTime = new DateTime(timestamp);
//        if (isTimeZone) {
//            TimeZone timeZone = TimeZone.getTimeZone(ZoneId.of("+07:00"));
//            ReflectionTestUtils.setField(dorisConnector, "timezone", timeZone);
//            dateTime.setTimeZone(timeZone);
//        }
//        Object actualData = codec.fromTapValue(tapDateValue);
//        Object exceptionData = formatTapDateTime(dateTime, "yyyy-MM-dd");
//        Assertions.assertEquals(exceptionData, actualData);
//
//
//    }
//
//    @Test
//    void testUpdateDateCapabilitiesForTapYearWithoutZone() {
//        testUpdateDateCapabilitiesForTapYear(false);
//    }
//
//
//    @Test
//    void testUpdateDateCapabilitiesForTapYearWithZone() {
//        testUpdateDateCapabilitiesForTapYear(true);
//    }
//
//    public void testUpdateDateCapabilitiesForTapYear(boolean isTimeZone){
//        DorisConnector dorisConnector = new DorisConnector();
//        ConnectorFunctions connectorFunctions = new ConnectorFunctions();
//        TapCodecsRegistry codecRegistry = new TapCodecsRegistry();
//        dorisConnector.registerCapabilities(connectorFunctions, codecRegistry);
//        FromTapValueCodec codec = codecRegistry.getCustomFromTapValueCodec(TapYearValue.class);
//        TapYearValue tapYearValue = new TapYearValue();
//        Timestamp timestamp = new Timestamp(1713962329337L);
//        tapYearValue.setValue(new DateTime(timestamp));
//        DateTime dateTime = new DateTime(timestamp);
//        if (isTimeZone) {
//            tapYearValue.setOriginType("timestamp");
//            TimeZone timeZone = TimeZone.getTimeZone(ZoneId.of("+07:00"));
//            ReflectionTestUtils.setField(dorisConnector, "timezone", timeZone);
//            dateTime.setTimeZone(timeZone);
//        }
//        Object actualData = codec.fromTapValue(tapYearValue);
//        Object exceptionData = formatTapDateTime(dateTime, "yyyy");
//        Assertions.assertEquals(exceptionData, actualData);
//    }
//
//
//    @Test
//    void testQueryTimeZoneWithZone() throws Throwable {
//        DorisConfig dorisConfig = new DorisConfig();
//        String timeZone = "+07:00";
//        dorisConfig.setTimezone(timeZone);
//        dorisConfig.setJdbcDriver("com.mysql.cj.jdbc.Driver");
//        DorisJdbcContext dorisConnector = new DorisJdbcContext(dorisConfig);
//        TimeZone actualData = dorisConnector.queryTimeZone();
//        TimeZone exceptionData = TimeZone.getTimeZone(ZoneId.of(timeZone));
//        Assertions.assertEquals(exceptionData.toZoneId(), actualData.toZoneId());
//
//    }
    @Nested
    class TestStreamLoadPrivilege {
        @DisplayName("Test Stream Load Privilege when test success")
        @Test
        void test1() {
            try (MockedStatic<HttpUtil> httpClientMockedStatic = mockStatic(HttpUtil.class)) {
                httpClientMockedStatic.when(() -> {
                    HttpUtil.get(anyString());
                }).thenReturn("Doris</title>");
                DorisConfig dorisConfig = new DorisConfig();
                dorisConfig.setUseHTTPS(false);
                Consumer<TestItem> consumer = new Consumer<TestItem>() {
                    @Override
                    public void accept(TestItem testItem) {
                        Assertions.assertEquals("Stream Write",testItem.getItem());
                        Assertions.assertEquals(TestItem.RESULT_SUCCESSFULLY,testItem.getResult());
                    }
                };
                io.tapdata.connector.doris.DorisTest dorisTest = new io.tapdata.connector.doris.DorisTest(dorisConfig, consumer);
                dorisTest.testStreamLoadPrivilege();
            }
        }
        @DisplayName("Test Stream Load Privilege when test failed")
        @Test
        void test2() {
            try (MockedStatic<HttpUtil> httpClientMockedStatic = mockStatic(HttpUtil.class)) {
                httpClientMockedStatic.when(() -> {
                    HttpUtil.get(anyString());
                }).thenReturn("2222");
                DorisConfig dorisConfig = new DorisConfig();
                dorisConfig.setUseHTTPS(false);
                Consumer<TestItem> consumer = new Consumer<TestItem>() {
                    @Override
                    public void accept(TestItem testItem) {
                        Assertions.assertEquals("Stream Write",testItem.getItem());
                        Assertions.assertEquals(TestItem.RESULT_SUCCESSFULLY_WITH_WARN,testItem.getResult());
                    }
                };
                io.tapdata.connector.doris.DorisTest dorisTest = new io.tapdata.connector.doris.DorisTest(dorisConfig, consumer);
                dorisTest.testStreamLoadPrivilege();
            }
        }
        @DisplayName("Test Stream Load Privilege when test throw exception")
        @Test
        void test3() {
            try (MockedStatic<HttpUtil> httpClientMockedStatic = mockStatic(HttpUtil.class)) {
                httpClientMockedStatic.when(() -> {
                    HttpUtil.get(anyString());
                }).thenThrow(new RuntimeException("test"));
                DorisConfig dorisConfig = new DorisConfig();
                dorisConfig.setUseHTTPS(false);
                Consumer<TestItem> consumer = new Consumer<TestItem>() {
                    @Override
                    public void accept(TestItem testItem) {
                        System.out.println(testItem);
                    }
                };
                io.tapdata.connector.doris.DorisTest dorisTest = new io.tapdata.connector.doris.DorisTest(dorisConfig, consumer);
                dorisTest.testStreamLoadPrivilege();
            }
        }
        @DisplayName("Test Stream Load Privilege when test failed use https")
        @Test
        void test4() throws IOException {
            try (MockedStatic<io.tapdata.connector.doris.streamload.HttpUtil> tapdataHttpUtil = mockStatic(io.tapdata.connector.doris.streamload.HttpUtil.class)) {
                DorisConfig dorisConfig = new DorisConfig();
                dorisConfig.setUseHTTPS(true);
                dorisConfig.setDorisHttp("localhost:8080");
                CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
                HttpEntity httpEntity = mock(HttpEntity.class);
                CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);
                when(httpResponse.getEntity()).thenReturn(httpEntity);
                when(httpClient.execute(any())).thenReturn(httpResponse);
                tapdataHttpUtil.when(io.tapdata.connector.doris.streamload.HttpUtil::generationHttpClient).thenReturn(httpClient);
                Consumer<TestItem> consumer = new Consumer<TestItem>() {
                    @Override
                    public void accept(TestItem testItem) {
                        System.out.println(testItem);
                    }
                };
                io.tapdata.connector.doris.DorisTest dorisTest = new io.tapdata.connector.doris.DorisTest(dorisConfig, consumer);
                dorisTest.testStreamLoadPrivilege();
            }
        }

    }
}
