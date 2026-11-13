package io.tapdata.connector.paimon.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Spill 租约协议的引擎级审计（SPEC-paimon-spill-owner-isolation §4.7）。
 *
 * <p>事件写入 SLF4J logger {@value #LOGGER_NAME}，与 Paimon 自身日志（如 FileChannelManagerImpl）共用
 * Engine 提供的绑定；任务 TapLog 只由调用方写摘要。审计永不影响协议结果：sink 失败被吞掉。主机名只读环境变量
 * {@code HOSTNAME}，不做名称解析；不记录连接配置。
 */
public final class PaimonSpillAudit {

    public static final String LOGGER_NAME = "io.tapdata.connector.paimon.spill";
    static final String PROTOCOL = "tapdata-paimon-v2";
    static final long RATE_LIMIT_NANOS = TimeUnit.MINUTES.toNanos(10);
    private static final int MAX_RATE_LIMIT_KEYS = 1024;

    /** 日志级别；协议事件之外的操作性事件（构建/释放/回滚）与协议事件共用同一 severity 语义。 */
    enum Severity { INFO, WARN, ERROR }

    enum Event {
        LEASE_CREATED(Severity.INFO),
        LEASE_RETIRED(Severity.INFO),
        LEASE_RETAINED(Severity.WARN),
        LEASE_RESIDUE(Severity.WARN),
        LEASE_RECLAIMED(Severity.INFO),
        RECLAIM_FAILED(Severity.ERROR),
        RECLAIM_SKIPPED(Severity.INFO),
        ADMISSION_DENIED(Severity.WARN),
        LEGACY_LAYOUT_FOUND(Severity.WARN),
        IO_MANAGER_CREATED(Severity.INFO),
        IO_MANAGER_BUILD_FAILED(Severity.ERROR),
        SPILL_REGISTRATION_INCOMPLETE(Severity.ERROR),
        UNREGISTERED_SPILL_DIR(Severity.ERROR),
        RELEASE_INCOMPLETE(Severity.WARN),
        PROVISION_RETRY(Severity.INFO),
        PROVISION_ROLLED_BACK(Severity.WARN),
        PROVISION_LOCK_HELD(Severity.ERROR),
        PROVISION_EXHAUSTED(Severity.ERROR),
        RECLAIM_BUDGET_EXHAUSTED(Severity.WARN),
        NAMESPACE_LIST_FAILED(Severity.WARN);

        final Severity severity;

        Event(Severity severity) {
            this.severity = severity;
        }
    }

    @FunctionalInterface
    interface Sink {
        void emit(Event event, Map<String, Object> fields, Throwable failure);
    }

    private static final PaimonSpillAudit DEFAULT =
            new PaimonSpillAudit(slf4jSink(), System::nanoTime, System::getenv);

    private final Sink sink;
    private final LongSupplier nanoClock;
    private final Function<String, String> environment;
    private final Map<String, Long> lastEmission = new ConcurrentHashMap<>();

    PaimonSpillAudit(Sink sink, LongSupplier nanoClock, Function<String, String> environment) {
        this.sink = sink;
        this.nanoClock = nanoClock;
        this.environment = environment;
    }

    static PaimonSpillAudit defaultAudit() {
        return DEFAULT;
    }

    void record(Event event, Map<String, Object> details) {
        record(event, details, null);
    }

    void record(Event event, Map<String, Object> details, Throwable failure) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("protocol", PROTOCOL);
        fields.put("pid", Identity.PID);
        fields.put("host", hostname());
        fields.put("jvmId", jvmIdOrUnknown());
        fields.put("classLoader", Identity.CLASS_LOADER);
        if (details != null) {
            fields.putAll(details);
        }
        try {
            sink.emit(event, Collections.unmodifiableMap(fields), failure);
        } catch (RuntimeException | LinkageError ignored) {
            // 审计不得改变协议结果，也不得把日志后端的故障传播到 owner 或回收者。
        }
    }

    /** 同一（事件, key）在 {@link #RATE_LIMIT_NANOS} 内只输出一次；返回本次是否输出。 */
    boolean recordRateLimited(Event event, String key, Map<String, Object> details) {
        return recordRateLimited(event, key, details, null);
    }

    boolean recordRateLimited(Event event, String key, Map<String, Object> details, Throwable failure) {
        long now = nanoClock.getAsLong();
        String rateKey = event + "|" + key;
        boolean[] emitted = {false};
        if (lastEmission.size() >= MAX_RATE_LIMIT_KEYS && !lastEmission.containsKey(rateKey)) {
            lastEmission.clear();
        }
        lastEmission.compute(rateKey, (k, previous) -> {
            if (previous != null && now - previous < RATE_LIMIT_NANOS) {
                return previous;
            }
            emitted[0] = true;
            return now;
        });
        if (emitted[0]) {
            record(event, details, failure);
        }
        return emitted[0];
    }

    static Map<String, Object> details(Object... keyValues) {
        Map<String, Object> details = new LinkedHashMap<>();
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            details.put(String.valueOf(keyValues[i]), keyValues[i + 1]);
        }
        return details;
    }

    private String hostname() {
        String host = environment.apply("HOSTNAME");
        return host == null || host.trim().isEmpty() ? "unknown" : host.trim();
    }

    private static String jvmIdOrUnknown() {
        try {
            return PaimonJvmIdentity.current();
        } catch (RuntimeException failure) {
            return "unknown";
        }
    }

    private static Sink slf4jSink() {
        Logger logger;
        try {
            logger = LoggerFactory.getLogger(LOGGER_NAME);
        } catch (RuntimeException | LinkageError unavailable) {
            return (event, fields, failure) -> { };
        }
        return (event, fields, failure) -> {
            if (failure == null) {
                log(logger, event, "Paimon spill {} {}", event, fields);
            } else {
                log(logger, event, "Paimon spill {} {}", event, fields, failure);
            }
        };
    }

    private static void log(Logger logger, Event event, String format, Object... arguments) {
        switch (event.severity) {
            case ERROR:
                logger.error(format, arguments);
                break;
            case WARN:
                logger.warn(format, arguments);
                break;
            default:
                logger.info(format, arguments);
                break;
        }
    }

    /** 进程号与 ClassLoader 标识在审计中只做诊断，读取失败不影响协议。 */
    private static final class Identity {
        private static final Object PID = pidOrUnknown();
        private static final String CLASS_LOADER = classLoaderId();

        private static Object pidOrUnknown() {
            try {
                return PaimonJvmIdentity.pidOf(ManagementFactory.getRuntimeMXBean());
            } catch (RuntimeException failure) {
                return "unknown";
            }
        }

        private static String classLoaderId() {
            ClassLoader loader = PaimonSpillAudit.class.getClassLoader();
            return loader == null ? "bootstrap"
                    : loader.getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(loader));
        }
    }
}
