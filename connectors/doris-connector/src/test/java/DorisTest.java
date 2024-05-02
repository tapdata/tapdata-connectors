import io.tapdata.connector.doris.DorisConnector;
import io.tapdata.connector.doris.DorisJdbcContext;
import io.tapdata.connector.doris.bean.DorisConfig;
import io.tapdata.entity.codec.FromTapValueCodec;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.entity.schema.value.TapDateTimeValue;
import io.tapdata.entity.schema.value.TapDateValue;
import io.tapdata.entity.schema.value.TapYearValue;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.TimeZone;

import static io.tapdata.base.ConnectorBase.formatTapDateTime;

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
}
