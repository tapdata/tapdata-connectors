package io.tapdata.connector.hudi.util;

import io.tapdata.entity.logger.Log;
import io.tapdata.kit.ErrorKit;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/1/11 18:28 Create
 * @version v2.0 2024/1/12 14:41 update by Gavin'Xiao
 */
public class AutoExpireInstance<InitKey, K, V> implements AutoCloseable {
    private final long cacheTimes;
    private final Function<InitKey, V> initFun;

    private final Function<InitKey, K> generateKey;
    private final BiConsumer<K, V> destroyFun;
    private final Map<K, Item<V>> map = new ConcurrentHashMap<>();
    private final ScheduledExecutorService cleanService = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledFuture<?> cleanFuture;
    private final Log log;
    private final AtomicBoolean running;
    public AutoExpireInstance(Function<InitKey, K> generateKey,
                              Function<InitKey, V> initFun,
                              BiConsumer<K, V> destroyFun,
                              Time time,
                              long cacheTimes, Log log) {
        this.log = log;
        this.running = new AtomicBoolean(true);;
        this.cacheTimes = cacheTimes;
        this.generateKey = generateKey;
        this.initFun = initFun;
        this.destroyFun = destroyFun;
        this.cleanFuture = cleanService.scheduleWithFixedDelay(this::runExpire, time.getInitialDelay(), time.getInterval(), time.getUnit());
    }

    public void call(InitKey key, Consumer<V> consumer) {
        K mapKey = generateKey.apply(key);
        Item<V> item = map.computeIfAbsent(mapKey, k -> new Item<>(0, initFun.apply(key)));
        item.lasted = System.currentTimeMillis();
        try {
            item.useCounts.addAndGet(1);
            consumer.accept(item.value);
        } finally {
            item.useCounts.addAndGet(-1);
            item.lasted = System.currentTimeMillis();
        }
    }

    private void runExpire() {
        for (K k : map.keySet()) {
            if (!running.get()) break;
            map.computeIfPresent(k, (key, val) -> {
                if (val.useCounts.get() == 0 && System.currentTimeMillis() - val.lasted > this.cacheTimes) {
                    doDestroyFun(key, val.value);
                    return null;
                }
                return val;
            });
        }
    }

    private void doDestroyFun(K key, V value) {
        try {
            destroyFun.accept(key, value);
        } catch (Exception e) {
            log.warn("Occur an error when close: {} - {}, message: {}", key, value, e.getMessage());
        }
    }

    @Override
    public void close() {
        running.compareAndSet(true, false);
        Optional.ofNullable(cleanFuture).ifPresent(e -> ErrorKit.ignoreAnyError(() -> e.cancel(true)));
        ErrorKit.ignoreAnyError(cleanService::shutdown);
        for (K k : map.keySet()) {
            map.computeIfPresent(k, (key, val) -> {
                doDestroyFun(key, val.value);
                return null;
            });
        }
        map.clear();
    }

    static class Item<V> {
        AtomicInteger useCounts = new AtomicInteger(0);
        long lasted;
        V value;

        public Item(long lasted, V value) {
            this.lasted = lasted;
            this.value = value;
        }
    }

    public static class Time {
        private final static long DEFAULT_EXPIRE_TIMES = 10;
        private long initialDelay;
        private final static long DEFAULT_INTERVAL = 10;
        private long interval;
        private final static TimeUnit DEFAULT_TIME_UTIL = TimeUnit.MINUTES;
        private TimeUnit unit;
        Time() {}
        public static Time defaultTime() {
            return new Time().withExpireTimes(DEFAULT_EXPIRE_TIMES).withInterval(DEFAULT_INTERVAL).withTimeUtil(DEFAULT_TIME_UTIL);
        }
        public static Time time(long initialDelay, long interval, TimeUnit timeUtil) {
            return new Time().withExpireTimes(initialDelay).withInterval(interval).withTimeUtil(timeUtil);
        }
        public Time withExpireTimes(long initialDelay) {
            if (initialDelay < 0 || initialDelay >= 999999999) {
                initialDelay = DEFAULT_EXPIRE_TIMES;
            }
            this.initialDelay = initialDelay;
            return this;
        }
        public Time withInterval(long interval) {
            if (interval <= 0 || interval >= 999999999) {
                interval = DEFAULT_INTERVAL;
            }
            this.interval = interval;
            return this;
        }
        public Time withTimeUtil(TimeUnit timeUtil) {
            if (null == timeUtil) {
                timeUtil = DEFAULT_TIME_UTIL;
            }
            this.unit = timeUtil;
            return this;
        }

        public long getInitialDelay() {
            return initialDelay;
        }

        public long getInterval() {
            return interval;
        }

        public TimeUnit getUnit() {
            return unit;
        }
    }
}
