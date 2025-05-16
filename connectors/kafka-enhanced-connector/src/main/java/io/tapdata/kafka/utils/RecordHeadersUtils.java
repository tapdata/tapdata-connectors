package io.tapdata.kafka.utils;

import io.tapdata.entity.mapping.TapEntry;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Kafka 消息头工具类
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/9/18 12:19 Create
 */
public interface RecordHeadersUtils {

    static List<byte[]> get(Headers headers, String key) {
        List<byte[]> values = new ArrayList<>();

        Iterable<Header> headerIterable = headers.headers(key);
        if (null == headerIterable) {
            return values;
        }

        headerIterable.forEach(header -> values.add(header.value()));
        return values;
    }

    static <T> List<T> get(Headers headers, String key, Function<byte[], T> fn) {
        List<T> values = new ArrayList<>();

        Iterable<Header> headerIterable = headers.headers(key);
        if (null == headerIterable) {
            return values;
        }

        headerIterable.forEach(header -> {
            T val = null;
            byte[] value = header.value();
            if (null != value) {
                val = fn.apply(value);
            }
            values.add(val);
        });
        return values;
    }

    static byte[] getLast(Headers headers, String key) {
        return Optional.ofNullable(headers.lastHeader(key))
            .map(heard -> null == heard.value() ? null : heard.value())
            .orElse(null);
    }

    static <T> T getLast(Headers headers, String key, Function<byte[], T> fn) {
        return Optional.ofNullable(headers.lastHeader(key)).map(Header::value).map(fn).orElse(null);
    }

    static String getLastString(Headers headers, String key) {
        return getLast(headers, key, String::new);
    }

    static Long getLastLong(Headers headers, String key) {
        return getLast(headers, key, bytes -> ByteBuffer.wrap(bytes).getLong());
    }

    static List<TapEntry<String, byte[]>> toList(Headers headers) {
        List<TapEntry<String, byte[]>> list = new ArrayList<>();
        headers.forEach(header -> list.add(new TapEntry<>(header.key(), header.value())));
        return list;
    }

    static Headers fromList(List<TapEntry<String, byte[]>> list) {
        Headers headers = new RecordHeaders();
        if (null != list) {
            list.forEach(header -> headers.add(header.getKey(), header.getValue()));
        }
		return headers;
    }
}
