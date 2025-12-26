package io.tapdata.connector.postgres.cdc.accept;

import io.tapdata.connector.postgres.cdc.NormalRedo;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.kit.EmptyKit;

import java.util.ArrayList;
import java.util.Collections;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/10 16:11 Create
 * @description
 */
public class LogMinerProBatchAccepter extends LogMinerBatchAccepter {
    @Override
    public void accept(NormalRedo redo) {
        if (EmptyKit.isNotNull(redo)) {
            if (EmptyKit.isNotNull(redo.getOperation())) {
                lastRedo = redo;
                TapEvent e = eventCreator.apply(redo);
                if (null == e) {
                    return;
                }
                events.add(e);
                if (events.size() >= getBatchSize()) {
                    getConsumer().accept(events, redo.getCdcSequenceStr());
                    events = new ArrayList<>();
                }
            } else {
                consumer.accept(Collections.singletonList(new HeartbeatEvent().init().referenceTime(System.currentTimeMillis())), redo.getCdcSequenceStr());
            }
        } else {
            if (!events.isEmpty()) {
                getConsumer().accept(events, lastRedo.getCdcSequenceStr());
                events = new ArrayList<>();
            }
        }
    }
}
