package io.tapdata.connector.postgres.partition.wrappper;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.partition.type.TapPartitionHash;
import io.tapdata.entity.schema.partition.type.TapPartitionStage;
import io.tapdata.entity.schema.partition.type.TapPartitionType;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HashWrapper extends PGPartitionWrapper {
    //for values with (modulus 4, remainder 0)
    public static final String REGEX = "modulus\\s+(\\d+)\\s*,\\s*remainder\\s+(\\d+)";

    @Override
    public TapPartitionStage type() {
        return TapPartitionStage.HASH;
    }

    @Override
    public List<TapPartitionType> parse(TapTable table, String partitionSQL, String checkOrPartitionRule, Log log) {
        //for values with (modulus 4, remainder 1);
        Pattern pattern = Pattern.compile(REGEX);
        Matcher matcher = pattern.matcher(String.valueOf(partitionSQL).toLowerCase());
        if (matcher.find()) {
            int modulus = Integer.parseInt(matcher.group(1));
            int remainder = Integer.parseInt(matcher.group(2));

            TapPartitionHash tapPartitionHash = new TapPartitionHash()
                    .modulus(modulus)
                    .remainder(remainder);
            List<TapPartitionType> hashes = new ArrayList<>();
            hashes.add(tapPartitionHash);
            return hashes;
        } else {
            throw new CoreException("Unable get modulus and remainder value from HASH partition table: {}, partition SQL: {}", table.getId(), partitionSQL);
        }
    }
}
