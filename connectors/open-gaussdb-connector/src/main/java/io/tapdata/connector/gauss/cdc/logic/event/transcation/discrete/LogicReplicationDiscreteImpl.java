package io.tapdata.connector.gauss.cdc.logic.event.transcation.discrete;

import com.huawei.opengauss.jdbc.replication.LogSequenceNumber;
import io.tapdata.connector.gauss.cdc.CdcOffset;
import io.tapdata.connector.gauss.cdc.logic.event.Event;
import io.tapdata.connector.gauss.cdc.logic.event.EventFactory;
import io.tapdata.connector.gauss.cdc.logic.event.LogicUtil;
import io.tapdata.connector.gauss.cdc.logic.event.dml.DeleteEvent;
import io.tapdata.connector.gauss.cdc.logic.event.dml.InsertEvent;
import io.tapdata.connector.gauss.cdc.logic.event.dml.UpdateEvent;
import io.tapdata.connector.gauss.cdc.logic.event.other.HeartBeatEvent;
import io.tapdata.connector.gauss.enums.CdcConstant;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class LogicReplicationDiscreteImpl extends EventFactory<ByteBuffer> {
    String sid;
    long sStartTime;
    final StreamReadConsumer eventAccept;
    final int batchSize;
    final CdcOffset offset;
    int transactionIndex = 0;

    private LogicReplicationDiscreteImpl(StreamReadConsumer consumer, int batchSize, CdcOffset offset) {
        this.eventAccept = consumer;
        if (batchSize > CdcConstant.CDC_MAX_BATCH_SIZE || batchSize <= CdcConstant.CDC_MIN_BATCH_SIZE) batchSize = CdcConstant.CDC_DEFAULT_BATCH_SIZE;
        this.batchSize = batchSize;
        if (null == offset) offset = new CdcOffset();
        this.offset = offset;
    }

    public static EventFactory<ByteBuffer> instance(StreamReadConsumer consumer, int batchSize, CdcOffset offset) {
        return new LogicReplicationDiscreteImpl(consumer, batchSize, offset);
    }

    @Override
    public void emit(ByteBuffer logEvent) {
        List<TapEvent> eventList = new ArrayList<>();
        try {
            while (logEvent.hasRemaining()) {
                byte[] pOrT = LogicUtil.read(logEvent, CdcConstant.BYTES_COUNT_BUFF_START);
                byte[] lsn = LogicUtil.read(logEvent, CdcConstant.BYTES_COUNT_LSN);
                LogSequenceNumber sequenceNumber = LogSequenceNumber.valueOf(LogicUtil.bytesToLong(lsn));
                offset.setLsn(sequenceNumber);
                byte[] type = LogicUtil.read(logEvent, CdcConstant.BYTES_COUNT_EVENT_TYPE);
                String transactionType = new String(type);
                if (transactionType.equalsIgnoreCase(CdcConstant.HEART_TAG)) {
                    break;
                }
                Event.EventEntity<TapEvent> eventEntity = redirect(logEvent, transactionType);
                if (null == eventEntity) continue;
                TapEvent event = eventEntity.event();
                if (null == event) continue;
                if (event instanceof TapRecordEvent) {
                    ((TapRecordEvent)event).setReferenceTime(offset.getTransactionTimestamp());
                }
                eventList.add(event);

                if (batchSize == eventList.size()) {
                    eventAccept.accept(eventList, offset);
                    eventList = new ArrayList<>();
                }
                //event.add(eventEntity.event());
                //@todo 提交事物
                //if (redirect instanceof CommitTransaction) {
                //    accept();
                //}
                //@todo 事物事件超出预期
                //@todo 事物数太多
                //@todo 事物回滚
            }
        } finally {
            if (!eventList.isEmpty()) {
                eventAccept.accept(eventList, offset);
                eventList = new ArrayList<>();
            }
        }
    }

    @Override
    protected void process() {

    }

    private Event.EventEntity<TapEvent> redirect(ByteBuffer logEvent, String type) {
        Event.EventEntity<TapEvent> event = null;
        switch (type) {
            case "B":
                offset.withXidIndex(-1);
                event = BeginTransaction.instance().analyze(logEvent);
                offset.setLsn(event.csn());
                offset.withTransactionTimestamp(event.timestamp());
                break;
            case "I":
                event = InsertEvent.instance().analyze(logEvent);
                advanceTransactionOffset();
                break;
            case "U":
                event = UpdateEvent.instance().analyze(logEvent);
                advanceTransactionOffset();
                break;
            case "D":
                event = DeleteEvent.instance().analyze(logEvent);
                advanceTransactionOffset();
                break;
            case "C":
                transactionIndex = 0;
                event = CommitTransaction.instance().analyze(logEvent);
                offset.withXidIndex(0);
                break;
            case CdcConstant.HEART_TAG_LOW:
            case CdcConstant.HEART_TAG:
                event = HeartBeatEvent.instance().analyze(logEvent);
                break;
        }
        return event;
    }

    @Override
    protected void accept() {
    }

    private boolean advanceTransactionOffset() {
        transactionIndex++;
        if (offset.getXidIndex() <= transactionIndex) {
            offset.withXidIndex(transactionIndex);
            return true;
        }
        return false;
    }
}
