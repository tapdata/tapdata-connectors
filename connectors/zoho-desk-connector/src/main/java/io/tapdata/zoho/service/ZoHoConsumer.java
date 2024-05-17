package io.tapdata.zoho.service;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.zoho.entity.ZoHoOffset;

import java.util.List;
import java.util.function.BiConsumer;

public class ZoHoConsumer implements BiConsumer<List<TapEvent>, Object> {
    protected BiConsumer<List<TapEvent>, Object> consumer;

    public ZoHoConsumer(BiConsumer<List<TapEvent>, Object> consumer) {
        this.consumer = consumer;
    }

    @Override
    public void accept(List<TapEvent> events, Object o) {
        Object offset = o instanceof ZoHoOffset ? ((ZoHoOffset) o).offset() : o;
        this.consumer.accept(events, offset);
    }

    @Override
    public BiConsumer<List<TapEvent>, Object> andThen(BiConsumer<? super List<TapEvent>, ? super Object> after) {
        return this.consumer.andThen(after);
    }
}
