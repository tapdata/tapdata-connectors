package io.tapdata.mongodb.reader.cdc.v4;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.mongodb.reader.MongodbV4StreamReader;
import io.tapdata.pdk.apis.consumer.StreamReadOneByOneConsumer;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/8 17:30 Create
 * @description
 */
public class OneByOneV4Acceptor extends V4Accept<OneByOneV4Acceptor, StreamReadOneByOneConsumer> {
    StreamReadOneByOneConsumer consumer;

    @Override
    public void accept(MongodbV4StreamReader.OffsetEvent e) {
        if (null == e || e.getEvent() == null) {
            return;
        }
        this.consumer.accept(e.getEvent(), e.getOffset());
    }

    @Override
    public void accept(List<TapEvent> e, Object offset) {
        if (CollectionUtils.isEmpty(e)) {
            return;
        }
        this.consumer.accept(e, offset);
    }

    @Override
    public OneByOneV4Acceptor setConsumer(StreamReadOneByOneConsumer consumer) {
        this.consumer = consumer;
        return this;
    }

    @Override
    public OneByOneV4Acceptor setBatchSize(int size) {
        return this;
    }

    @Override
    public OneByOneV4Acceptor setBatchSizeTimeout(long ms) {
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
