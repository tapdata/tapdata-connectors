package io.tapdata.connector.mysql.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.DatabaseMetaData;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MysqlVersionUtilTest {
    @ParameterizedTest
    @CsvSource({
            "MySQL, 5.7.44, 5, 7, false",
            "MySQL, 8.0.42, 8, 0, false",
            "MySQL, 8.3.0, 8, 3, false",
            "MySQL, 8.4.0, 8, 4, true",
            "MySQL, 9.0.0, 9, 0, true",
            "MySQL, 10.11.14-MariaDB, 10, 11, false",
            "MySQL, 12.0.2-MariaDB, 12, 0, false",
            "MariaDB, 12.0.2, 12, 0, false",
            "mariadb, 12.0.2, 12, 0, false"
    })
    void selectsStatusSyntaxByServerFamily(String product, String version, int major, int minor,
                                          boolean modernSyntax) throws Exception {
        DatabaseMetaData metadata = mock(DatabaseMetaData.class);
        when(metadata.getDatabaseProductName()).thenReturn(product);
        when(metadata.getDatabaseProductVersion()).thenReturn(version);
        when(metadata.getDatabaseMajorVersion()).thenReturn(major);
        when(metadata.getDatabaseMinorVersion()).thenReturn(minor);
        assertEquals(modernSyntax, MysqlVersionUtil.usesBinaryLogStatus(metadata));
    }
}
