package io.tapdata.connector.gauss.cdc.logic.event.transcation.complex;

import io.tapdata.connector.gauss.cdc.logic.event.Event;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;
import io.tapdata.entity.event.dml.TapRecordEvent;

import java.nio.ByteBuffer;
import java.util.List;

public class TransactionComplex implements Event<List<TapRecordEvent>> {
    @Override
    public EventEntity<List<TapRecordEvent>> process(ByteBuffer logEvent, EventParam processParam) {
        return null;
    }

    @Override
    public Event.EventEntity<List<TapRecordEvent>> analyze(ByteBuffer logEvent) {
        return null;
    }
}