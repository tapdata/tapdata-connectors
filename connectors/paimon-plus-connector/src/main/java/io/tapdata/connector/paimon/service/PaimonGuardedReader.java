package io.tapdata.connector.paimon.service;

import org.apache.paimon.reader.RecordReader;
import java.io.IOException;

/** 有限 reader 的动作准入与独立资源账本；permit 退出不代表 reader 已关闭。 */
public final class PaimonGuardedReader<T> implements RecordReader<T> {
    private final RecordReader<T> delegate;
    private final PaimonStopResources.Scope scope;
    private final boolean completesScope;
    private final java.util.Set<RecordIterator<T>> outstanding = new java.util.LinkedHashSet<>();
    public PaimonGuardedReader(RecordReader<T> delegate, PaimonStopResources.Scope scope) {
        this(delegate, scope, true);
    }
    public PaimonGuardedReader(RecordReader<T> delegate, PaimonStopResources.Scope scope, boolean completesScope) {
        this.delegate = delegate; this.scope = scope; this.completesScope = completesScope;
    }
    private <R> R call(String action, PaimonStopController.CheckedSupplier<R> body) throws IOException {
        try { return scope.call(action, body); }
        catch (IOException | RuntimeException failure) { throw failure; }
        catch (Exception failure) { throw new IOException(failure); }
    }
    @Override public RecordIterator<T> readBatch() throws IOException {
        RecordIterator<T> batch = call("reader readBatch", delegate::readBatch);
        if (batch == null) { return null; }
        PaimonStopResources.Slot slot = scope.reserve("reader batch");
        slot.bind(batch);
        RecordIterator<T> guarded = new RecordIterator<T>() {
            @Override public T next() throws IOException { return call("reader next", batch::next); }
            @Override public void releaseBatch() {
                try { call("reader releaseBatch", () -> { batch.releaseBatch(); return null; }); }
                catch (IOException failure) { scope.retain(failure); throw new java.io.UncheckedIOException(failure); }
                catch (RuntimeException | Error failure) { scope.retain(failure); throw failure; }
                slot.released();
                outstanding.remove(this);
            }
        };
        outstanding.add(guarded);
        return guarded;
    }
    @Override public void close() throws IOException {
        try {
            // query limit、解码/回调异常都可能在 batch 中途退出。RecordReader 接口要求
            // 调用者 releaseBatch，不能推断 delegate.close 等价于释放所有 iterator。
            // https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-common/src/main/java/org/apache/paimon/reader/RecordReader.java#L59
            for (RecordIterator<T> batch : new java.util.ArrayList<>(outstanding)) { batch.releaseBatch(); }
            call("reader close", () -> { delegate.close(); return null; });
            scope.check("publish reader close proof");
            if (completesScope) { scope.completed(); }
        } catch (IOException | RuntimeException | Error failure) {
            scope.retain(failure);
            throw failure;
        }
    }
}
