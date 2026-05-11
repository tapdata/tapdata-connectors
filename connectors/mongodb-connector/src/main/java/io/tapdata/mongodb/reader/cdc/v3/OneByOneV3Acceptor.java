package io.tapdata.mongodb.reader.cdc.v3;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.mongodb.reader.v3.TapEventOffset;
import io.tapdata.pdk.apis.consumer.StreamReadOneByOneConsumer;
import org.apache.commons.collections4.CollectionUtils;

import java.util.HashMap;
import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/8 17:30 Create
 * @description
 */
public class OneByOneV3Acceptor extends V3Accept<OneByOneV3Acceptor, StreamReadOneByOneConsumer> {
    StreamReadOneByOneConsumer consumer;

    @Override
    public void accept(TapEventOffset e) {
        if (e == null || null == e.getTapEvent()) {
            return;
        }
        this.offset.put(e.getReplicaSetName(), e.getOffset());
        consumer.accept(e.getTapEvent(), new HashMap<>(offset));
    }

    @Override
    public void accept(List<TapEvent> e, Object offset) {
        if (CollectionUtils.isEmpty(e)) {
            return;
        }
        consumer.accept(e, offset);
    }

    @Override
    public OneByOneV3Acceptor setConsumer(StreamReadOneByOneConsumer consumer) {
        this.consumer = consumer;
        return this;
    }

    @Override
    public OneByOneV3Acceptor setBatchSize(int size) {
        return this;
    }

    @Override
    public OneByOneV3Acceptor setBatchSizeTimeout(long ms) {
        return this;
    }

    @Override
    public void streamReadStarted() {
        this.consumer.streamReadStarted();
    }

    @Override
    public void streamReadEnded() {
        this.consumer.streamReadEnded();
    }

    @Override
    public StreamReadOneByOneConsumer getConsumer() {
        return this.consumer;
    }


}
