package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.MariadbGtidEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * Mariadb GTID_EVENT Fields
 * <pre>
 *     uint8 GTID sequence
 *     uint4 Replication Domain ID
 *     uint1 Flags
 *
 * 	if flag &amp; FL_GROUP_COMMIT_ID
 * 	    uint8 commit_id
 * 	else
 * 	    uint6 0
 * </pre>
 *
 * @author <a href="mailto:winger2049@gmail.com">Winger</a>
 * @see <a href="https://mariadb.com/kb/en/gtid_event/">GTID_EVENT</a> for the original doc
 */
public class MariadbGtidEventDataDeserializer implements EventDataDeserializer<MariadbGtidEventData> {
    @Override
    public MariadbGtidEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        MariadbGtidEventData event = new MariadbGtidEventData();
        event.setSequence(inputStream.readLong(8));
        event.setDomainId(inputStream.readInteger(4));
        event.setFlags(inputStream.readInteger(1));
        // Flags ignore
        return event;
    }
}
