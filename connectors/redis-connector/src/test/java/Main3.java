import io.tapdata.connector.redis.pipeline.ClusterExtPipeline;
import redis.clients.jedis.*;
import redis.clients.jedis.commands.ProtocolCommand;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static redis.clients.jedis.Protocol.Command.*;
import static redis.clients.jedis.Protocol.toByteArray;

public class Main3 {
    public static void main(String[] args) {
        Set<HostAndPort> nodes = new HashSet<>(Arrays.asList(new HostAndPort("119.29.142.248", 8380)));
        DefaultJedisClientConfig.Builder clientConfigBuilder = DefaultJedisClientConfig.builder()
                .database(0)
                .blockingSocketTimeoutMillis(60000)
                .connectionTimeoutMillis(5000)
                .socketTimeoutMillis(5000);
        clientConfigBuilder.password("supercoolGJ0628@cpic");
        try (
                ClusterExtPipeline pipeline = new ClusterExtPipeline(nodes, clientConfigBuilder.build());
        ) {
            pipeline.sendCommand(SET, "test".getBytes(StandardCharsets.UTF_8), "test3".getBytes(StandardCharsets.UTF_8));
            pipeline.sync();
        }
    }
}
