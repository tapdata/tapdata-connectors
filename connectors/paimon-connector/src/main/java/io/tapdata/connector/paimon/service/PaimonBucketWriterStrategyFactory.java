package io.tapdata.connector.paimon.service;

import org.apache.paimon.table.BucketMode;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * Exhaustive Paimon 1.3.1 bucket-mode strategy selector.
 *
 * <p>The explicit mapping intentionally follows Paimon's own exhaustive sink selection and never
 * falls back to native writes for an unknown future mode:
 * https://github.com/apache/paimon/blob/release-1.3.1/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/FlinkSinkBuilder.java
 */
final class PaimonBucketWriterStrategyFactory {

    private static final Set<BucketMode> SUPPORTED_MODES =
            Collections.unmodifiableSet(
                    EnumSet.of(
                            BucketMode.HASH_FIXED,
                            BucketMode.HASH_DYNAMIC,
                            BucketMode.KEY_DYNAMIC,
                            BucketMode.POSTPONE_MODE,
                            BucketMode.BUCKET_UNAWARE));

    private PaimonBucketWriterStrategyFactory() {
    }

    static Set<BucketMode> supportedModes() {
        return SUPPORTED_MODES;
    }

    static boolean requiresIoManager(BucketMode mode) {
        return Objects.requireNonNull(mode, "mode") == BucketMode.KEY_DYNAMIC;
    }

    static boolean requiresOrderedSingleWriterIngress(BucketMode mode) {
        BucketMode checked = Objects.requireNonNull(mode, "mode");
        return checked == BucketMode.HASH_DYNAMIC || checked == BucketMode.KEY_DYNAMIC;
    }

    static PaimonBucketWriterStrategy create(PaimonBucketWriterStrategyContext context)
            throws Exception {
        return create(context, DefaultPaimonBucketWriterRuntimeFactory.INSTANCE);
    }

    static PaimonBucketWriterStrategy create(
            PaimonBucketWriterStrategyContext context,
            PaimonBucketWriterRuntimeFactory runtimeFactory)
            throws Exception {
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(runtimeFactory, "runtimeFactory");
        BucketMode mode = context.table().bucketMode();
        switch (mode) {
            case HASH_FIXED:
                return new HashFixedBucketWriterStrategy(context);
            case HASH_DYNAMIC:
                return new HashDynamicBucketWriterStrategy(context, runtimeFactory);
            case KEY_DYNAMIC:
                return new KeyDynamicBucketWriterStrategy(context, runtimeFactory);
            case POSTPONE_MODE:
                return new PostponeBucketWriterStrategy(context);
            case BUCKET_UNAWARE:
                return new BucketUnawareWriterStrategy(context);
            default:
                throw new UnsupportedOperationException("Unsupported Paimon bucket mode: " + mode);
        }
    }
}
