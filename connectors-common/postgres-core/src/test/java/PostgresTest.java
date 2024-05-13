import io.tapdata.connector.postgres.dml.PostgresWriteRecorder;
import io.tapdata.entity.schema.TapTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.Connection;

public class PostgresTest {

    @Test
    void filterValueTestForBoolean() {
        PostgresWriteRecorder postgresWriteRecorder = new PostgresWriteRecorder(
                Mockito.mock(Connection.class), Mockito.mock(TapTable.class), "test");

        int actualData = ReflectionTestUtils.invokeMethod(postgresWriteRecorder, "filterValue", true, "int");
        Assertions.assertTrue(actualData == 1);

    }
}
