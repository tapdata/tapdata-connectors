package io.tapdata.common.dml;

import io.tapdata.pdk.apis.entity.WriteListResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class NormalWriteRecorderTest {

    @Nested
    class AddUpdateBatchTest {

        NormalWriteRecorder recorder;
        PreparedStatement preparedStatement;

        @BeforeEach
        void beforeEach() throws SQLException {
            recorder = mock(NormalWriteRecorder.class);
            preparedStatement = mock(PreparedStatement.class);
            ReflectionTestUtils.setField(recorder, "preparedStatement", preparedStatement);
            ReflectionTestUtils.setField(recorder, "allColumn", Arrays.asList("id", "name"));
            ReflectionTestUtils.setField(recorder, "uniqueCondition", Collections.singletonList("id"));
            doAnswer(invocationOnMock -> null).when(preparedStatement).addBatch();
        }

        @Test
        void testNormal() throws SQLException {
            doAnswer(invocationOnMock -> null).when(recorder).insertUpdate(any(), any(), any());
            doAnswer(invocationOnMock -> null).when(recorder).insertUpdate(any(), any(), any());
            ReflectionTestUtils.setField(recorder, "updatePolicy", WritePolicyEnum.IGNORE_ON_NONEXISTS);
            Map<String, Object> after = map(entry("id", 1), entry("name", "name"));
            Map<String, Object> before = map(entry("id", 1));
            doCallRealMethod().when(recorder).addUpdateBatch(any(), any(), any());
            recorder.addUpdateBatch(after, before, new WriteListResult<>());
            verify(recorder, times(0)).insertUpdate(any(), any(), any());
            verify(recorder, times(1)).justUpdate(any(), any(), any());
        }

        @Test
        void testInsertUpdate() throws SQLException {
            doAnswer(invocationOnMock -> null).when(recorder).insertUpdate(any(), any(), any());
            doAnswer(invocationOnMock -> null).when(recorder).insertUpdate(any(), any(), any());
            ReflectionTestUtils.setField(recorder, "updatePolicy", WritePolicyEnum.INSERT_ON_NONEXISTS);
            Map<String, Object> after = map(entry("id", 1), entry("name", "name"));
            Map<String, Object> before = map(entry("id", 1));
            doCallRealMethod().when(recorder).addUpdateBatch(any(), any(), any());
            recorder.addUpdateBatch(after, before, new WriteListResult<>());
            verify(recorder, times(1)).insertUpdate(any(), any(), any());
            verify(recorder, times(0)).justUpdate(any(), any(), any());
        }

        @Test
        void testEmptyAfter() throws SQLException {
            doAnswer(invocationOnMock -> null).when(recorder).insertUpdate(any(), any(), any());
            doAnswer(invocationOnMock -> null).when(recorder).insertUpdate(any(), any(), any());
            ReflectionTestUtils.setField(recorder, "updatePolicy", WritePolicyEnum.INSERT_ON_NONEXISTS);
            Map<String, Object> after = Collections.emptyMap();
            Map<String, Object> before = map(entry("id", 1));
            doCallRealMethod().when(recorder).addUpdateBatch(any(), any(), any());
            recorder.addUpdateBatch(after, before, new WriteListResult<>());
            verify(recorder, times(0)).insertUpdate(any(), any(), any());
            verify(recorder, times(0)).justUpdate(any(), any(), any());
        }
    }
}
