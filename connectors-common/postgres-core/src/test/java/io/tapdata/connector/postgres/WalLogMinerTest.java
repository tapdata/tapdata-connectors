package io.tapdata.connector.postgres;

import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;

public class WalLogMinerTest {

    @Test
    void testParseTimestamp(){
        String timestamp = "2022-02-22 14:33:00.1234";
        Timestamp.valueOf(timestamp);
    }
}
