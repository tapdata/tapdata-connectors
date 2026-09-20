package io.tapdata.connector.paimon;

import org.apache.paimon.table.sink.TableCommitImpl;
import java.util.concurrent.Executors;
import static org.mockito.Mockito.*;

/** 原生 committer 替身持有独立执行器，不能用 null 伪造维护退出证明。 */
public final class NativeCommitterFixture {
    private NativeCommitterFixture() {}
    public static TableCommitImpl committer() {
        TableCommitImpl commit = mock(TableCommitImpl.class);
        when(commit.getMaintainExecutor()).thenReturn(Executors.newSingleThreadExecutor());
        return commit;
    }
}
