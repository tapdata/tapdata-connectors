package io.tapdata.connector.klustron.cdc;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.tapdata.connector.mysql.MysqlReaderV2;
import io.tapdata.connector.mysql.config.MysqlConfig;
import io.tapdata.connector.mysql.entity.MysqlBinlogPosition;
import io.tapdata.entity.logger.Log;
import io.tapdata.kit.EmptyKit;

import java.util.TimeZone;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/3/10 15:26 Create
 * @description
 */
public class KunLunBinLogReader extends MysqlReaderV2 {

    public KunLunBinLogReader(MysqlConfig config, Log tapLogger, TimeZone timeZone) {
        super(config, tapLogger, timeZone);
    }


    @Override
    protected Pair setStartOffset(BinaryLogClient client, Object offset) {
        // set start point
        if (offset instanceof MysqlBinlogPosition) {
            MysqlBinlogPosition position = (MysqlBinlogPosition) offset;
            if (null != position.getGtidSet()) {
                client.setGtidSet(position.getGtidSet());
                return new Pair(null, position.getGtidSet());
            } else if (EmptyKit.isNotEmpty(position.getFilename())) {
                client.setBinlogFilename(position.getFilename());
                client.setBinlogPosition(position.getPosition());
                tapLogger.info("Starting from binlog position: {}/{}", position.getFilename(), position.getPosition());
                return new Pair(position.getFilename(), null);
            }
        }
        return new Pair(null, null);
    }
}
