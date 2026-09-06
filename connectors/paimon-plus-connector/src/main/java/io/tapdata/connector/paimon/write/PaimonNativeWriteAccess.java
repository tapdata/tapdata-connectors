package io.tapdata.connector.paimon.write;

import org.apache.paimon.operation.AbstractFileStoreWrite;
import org.apache.paimon.operation.FileStoreWrite;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.utils.RecordWriter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** 固定 1.3.2 的窄适配边界；升级必须重核 public @VisibleForTesting 接缝。 */
public final class PaimonNativeWriteAccess {
    private final AbstractFileStoreWrite<?> write;

    private PaimonNativeWriteAccess(AbstractFileStoreWrite<?> write) { this.write = write; }

    public static PaimonNativeWriteAccess of(StreamTableWrite rawWriter) {
        if (!(rawWriter instanceof TableWriteImpl)) {
            throw new IllegalArgumentException("Paimon writer type drift: expected TableWriteImpl");
        }
        // TableWriteImpl.getWrite 返回 FileStoreWrite 接口；只有显式验证实现类型后才能读取
        // AbstractFileStoreWrite.writers。不得反射、清空 map 或为未知实现返回空 writer 集合。
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/TableWriteImpl.java#L287
        // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java#L567
        FileStoreWrite<?> nativeWrite = ((TableWriteImpl<?>) rawWriter).getWrite();
        if (!(nativeWrite instanceof AbstractFileStoreWrite)) {
            throw new IllegalArgumentException("Paimon writer type drift: expected AbstractFileStoreWrite");
        }
        return new PaimonNativeWriteAccess((AbstractFileStoreWrite<?>) nativeWrite);
    }

    /** 仅供直接构造且没有原生 bucket 的测试策略；生产 Factory 必须调用 of。 */
    static PaimonNativeWriteAccess withoutNativeWriters() { return new PaimonNativeWriteAccess(null); }

    /** 调用前必须取得实际 executor termination；遍历全部 bucket，即使前一个失败。 */
    public List<Throwable> syncAll() {
        if (write == null) { return Collections.emptyList(); }
        List<RecordWriter<?>> writers = new ArrayList<>();
        for (Map<Integer, ? extends AbstractFileStoreWrite.WriterContainer<?>> buckets : write.writers().values()) {
            for (AbstractFileStoreWrite.WriterContainer<?> container : buckets.values()) {
                writers.add(container.writer);
            }
        }
        List<Throwable> errors = new ArrayList<>();
        for (RecordWriter<?> writer : writers) {
            try {
                // 原生 sync 只消费/应用已提交的 Compaction Future，不触发新任务；其 finally
                // 清空失败 Future，避免原生 close 在第一桶重复抛错而截断其他桶的清理。
                // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java#L276
                // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/compact/CompactFutureManager.java#L47
                writer.sync();
            } catch (Exception | Error failure) {
                errors.add(failure);
            }
        }
        return errors;
    }

    public static boolean hasFinalCompaction(List<CommitMessage> messages) {
        boolean nonEmpty = false;
        for (CommitMessage message : messages) {
            if (!(message instanceof CommitMessageImpl)) {
                throw new IllegalArgumentException("Paimon message type drift: expected CommitMessageImpl");
            }
            // CommitMessage 接口没有 increment/isEmpty；DataIncrement.isEmpty 包含 data、
            // changelog、index 的全部集合。最终 STOP 不能把未确认业务误当作 Compaction 提交。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/CommitMessageImpl.java#L79
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/io/DataIncrement.java#L83
            CommitMessageImpl nativeMessage = (CommitMessageImpl) message;
            if (!nativeMessage.newFilesIncrement().isEmpty()) {
                throw new IllegalStateException("Final Paimon Compaction contains unconfirmed business data");
            }
            nonEmpty |= !nativeMessage.isEmpty();
        }
        return nonEmpty;
    }
}
