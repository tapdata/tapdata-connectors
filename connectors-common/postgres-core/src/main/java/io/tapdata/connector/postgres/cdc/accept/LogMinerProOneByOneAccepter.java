package io.tapdata.connector.postgres.cdc.accept;

import io.tapdata.connector.postgres.cdc.NormalRedo;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.kit.EmptyKit;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2025/12/10 16:11 Create
 * @description
 */
public class LogMinerProOneByOneAccepter extends LogMinerOneByOneAccepter {
    @Override
    public void accept(NormalRedo redo) {
        if (EmptyKit.isNotNull(redo)) {
            TapEvent e = null;
            if (EmptyKit.isNotNull(redo.getOperation())) {
                e = eventCreator.apply(redo);
            } else {
                e = new HeartbeatEvent().init().referenceTime(System.currentTimeMillis());
            }
            if (null == e) {
                return;
            }
            getConsumer().accept(e, redo.getCdcSequenceStr());
        }
    }
}
