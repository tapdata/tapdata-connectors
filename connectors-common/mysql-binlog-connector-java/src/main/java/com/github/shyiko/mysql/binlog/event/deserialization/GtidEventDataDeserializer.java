/*
 * Copyright 2013 Patrick Prasse
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.MySqlGtid;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.UUID;

/**
 * @author <a href="mailto:pprasse@actindo.de">Patrick Prasse</a>
 */
public class GtidEventDataDeserializer implements EventDataDeserializer<GtidEventData> {

    @Override
    public GtidEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        byte flags = (byte) inputStream.readInteger(1);
        long sourceIdMostSignificantBits = readLongBigEndian(inputStream);
        long sourceIdLeastSignificantBits = readLongBigEndian(inputStream);
        long transactionId = inputStream.readLong(8);

        return new GtidEventData(
            new MySqlGtid(
                new UUID(sourceIdMostSignificantBits, sourceIdLeastSignificantBits),
                transactionId
            ),
            flags
        );
    }

    private static long readLongBigEndian(ByteArrayInputStream input) throws IOException {
        long result = 0;
        for (int i = 0; i < 8; ++i) {
            result = ((result << 8) | (input.read() & 0xff));
        }
        return result;
    }
}
