package io.tapdata.connector.gauss.cdc.logic.event.transcation.discrete;

import io.tapdata.connector.gauss.cdc.CdcOffset;
import io.tapdata.connector.gauss.cdc.logic.AnalyzeLog;
import io.tapdata.connector.gauss.cdc.logic.event.Event;
import io.tapdata.connector.gauss.cdc.logic.event.EventFactory;
import io.tapdata.connector.gauss.util.LogicUtil;
import io.tapdata.connector.gauss.cdc.logic.event.dml.DeleteEvent;
import io.tapdata.connector.gauss.cdc.logic.event.dml.InsertEvent;
import io.tapdata.connector.gauss.cdc.logic.event.dml.UpdateEvent;
import io.tapdata.connector.gauss.cdc.logic.event.other.HeartBeatEvent;
import io.tapdata.connector.gauss.entity.IllegalDataLengthException;
import io.tapdata.connector.gauss.enums.CdcConstant;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static io.tapdata.base.ConnectorBase.list;

public class LogicReplicationDiscreteImpl extends EventFactory<ByteBuffer> {
    protected StreamReadConsumer eventAccept;
    protected int batchSize;
    protected CdcOffset offset;
    protected int transactionIndex = 0;
    protected AnalyzeLog.AnalyzeParam param;
    protected Supplier<Boolean> supplier;

    private LogicReplicationDiscreteImpl(StreamReadConsumer consumer, int batchSize, CdcOffset offset, Supplier<Boolean> supplier) {
        this.eventAccept = consumer;
        if (batchSize > CdcConstant.CDC_MAX_BATCH_SIZE || batchSize <= CdcConstant.CDC_MIN_BATCH_SIZE) {
            batchSize = CdcConstant.CDC_DEFAULT_BATCH_SIZE;
        }
        this.batchSize = batchSize;
        if (null == offset) {
            offset = new CdcOffset();
        }
        this.offset = offset;
        this.param = new AnalyzeLog.AnalyzeParam();
        this.supplier = supplier;
    }

    public static EventFactory<ByteBuffer> instance(StreamReadConsumer consumer, int batchSize, CdcOffset offset, Supplier<Boolean> supplier) {
        return new LogicReplicationDiscreteImpl(consumer, batchSize, offset, supplier);
    }

    protected boolean hasNext(ByteBuffer buffer) {
        return null != supplier && supplier.get() && null != buffer && buffer.hasRemaining();
    }


    @Override
    public void emit(ByteBuffer logEvent, Log log) {
        if (null == logEvent) return;
        List<TapEvent> eventList = new ArrayList<>();
        try {
            while (hasNext(logEvent)) {
                byte[] pOrT = LogicUtil.read(logEvent, CdcConstant.BYTES_COUNT_BUFF_START);
                int allLength = LogicUtil.bytesToInt(pOrT);
                if (allLength <= 0) continue;
                byte[] lsn = LogicUtil.read(logEvent, CdcConstant.BYTES_COUNT_LSN);
                byte[] type = LogicUtil.read(logEvent, CdcConstant.BYTES_COUNT_EVENT_TYPE);
                String transactionType = new String(type);
                Event.EventEntity<TapEvent> eventEntity = redirect(logEvent, transactionType.toUpperCase());
                if (null == eventEntity) continue;
                TapEvent event = eventEntity.event();
                if (null == event) continue;
                if (event instanceof HeartbeatEvent) {
                    eventAccept.accept(list(event), offset);
                    break;
                }
                offset.setLsn(LogicUtil.byteToLong(lsn));
                if (event instanceof TapRecordEvent) {
                    ((TapRecordEvent)event).setReferenceTime(offset.getTransactionTimestamp());
                }
                eventList.add(event);

                if (batchSize == eventList.size()) {
                    eventAccept.accept(eventList, offset);
                    eventList = new ArrayList<>();
                }
            }
        } catch (Exception e) {
            if (e instanceof IllegalDataLengthException) {
                log.warn("{}, fail to read value from byte array: {}", e.getMessage(), new String(logEvent.array()));
                return;
            }
            throw e;
        } finally {
            if (!eventList.isEmpty()) {
                eventAccept.accept(eventList, offset);
                eventList = new ArrayList<>();
            }
        }
    }

    @Override
    protected void process() {
        throw new UnsupportedOperationException();
    }

    protected Event.EventEntity<TapEvent> redirect(ByteBuffer logEvent, String type) {
        if (null == logEvent) {
            return null;
        }
        if (null == type) {
            return null;
        }
        Event.EventEntity<TapEvent> event = null;
        switch (type) {
            case CdcConstant.BEGIN_TAG:
                offset.withXidIndex(-1);
                event = BeginTransaction.instance().analyze(logEvent, param);
                offset.setLsn(event.csn());
                offset.withTransactionTimestamp(event.timestamp());
                break;
            case CdcConstant.INSERT_TAG:
                event = InsertEvent.instance().analyze(logEvent, param);
                advanceTransactionOffset();
                break;
            case CdcConstant.UPDATE_TAG:
                event = UpdateEvent.instance().analyze(logEvent, param);
                advanceTransactionOffset();
                break;
            case CdcConstant.DELETE_TAG:
                event = DeleteEvent.instance().analyze(logEvent, param);
                advanceTransactionOffset();
                break;
            case CdcConstant.COMMIT_TAG:
                transactionIndex = 0;
                event = CommitTransaction.instance().analyze(logEvent, param);
                offset.withXidIndex(0);
                break;
            case CdcConstant.HEART_TAG:
                event = HeartBeatEvent.instance().analyze(logEvent, param);
                break;
            default:
                return null;
        }
        return event;
    }

    @Override
    protected void accept() {
        throw new UnsupportedOperationException();
    }

    protected boolean advanceTransactionOffset() {
        transactionIndex++;
        if (offset.getXidIndex() <= transactionIndex) {
            offset.withXidIndex(transactionIndex);
            return true;
        }
        return false;
    }
}
