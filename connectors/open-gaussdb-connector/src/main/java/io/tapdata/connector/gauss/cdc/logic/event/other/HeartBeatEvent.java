package io.tapdata.connector.gauss.cdc.logic.event.other;

import io.tapdata.connector.gauss.cdc.logic.event.Event;
import io.tapdata.connector.gauss.util.LogicUtil;
import io.tapdata.connector.gauss.cdc.logic.param.EventParam;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;

import java.nio.ByteBuffer;

public class HeartBeatEvent implements Event<TapEvent> {
    private HeartBeatEvent() {

    }
    public static HeartBeatEvent instance() {
        return new HeartBeatEvent();
    }
    @Override
    public EventEntity<TapEvent> process(ByteBuffer logEvent, EventParam processParam) {
        return null;
    }

    @Override
    public Event.EventEntity<TapEvent> analyze(ByteBuffer logEvent, AnalyzeParam param) {
        HeartbeatEvent event = new HeartbeatEvent();
        byte[] lsn1 = LogicUtil.read(logEvent, 8); //int64, 发送心跳逻辑日志时读取的WAL日志结束位置
        byte[] lsn2 = LogicUtil.read(logEvent, 8); //int64, 发送心跳逻辑日志时已经落盘的WAL日志的位置
        byte[] timestamp = LogicUtil.read(logEvent, 8); //int64, 最新解码到的事物日志或检查点日志的产生时间戳，yyyy-MM-dd HH:mm:ss
        byte[] stopChar = LogicUtil.read(logEvent, 1); //结束符 F
        long time = System.currentTimeMillis();
        event.referenceTime(time);
        event.setTime(time);
        return new EventEntity<>(event, "", time, 0, LogicUtil.byteToLong(lsn2));
    }
}
