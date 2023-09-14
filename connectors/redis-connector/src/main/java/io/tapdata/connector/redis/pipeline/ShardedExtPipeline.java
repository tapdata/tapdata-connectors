package io.tapdata.connector.redis.pipeline;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.providers.ShardedConnectionProvider;
import redis.clients.jedis.util.Hashing;

import java.util.List;
import java.util.regex.Pattern;

public class ShardedExtPipeline extends ShardedPipeline {

    public ShardedExtPipeline(List<HostAndPort> shards, JedisClientConfig clientConfig) {
        super(shards, clientConfig);
    }

    public ShardedExtPipeline(ShardedConnectionProvider provider) {
        super(provider);
    }

    public ShardedExtPipeline(List<HostAndPort> shards, JedisClientConfig clientConfig, GenericObjectPoolConfig<Connection> poolConfig, Hashing algo, Pattern tagPattern) {
        super(shards, clientConfig, poolConfig, algo, tagPattern);
    }

    public ShardedExtPipeline(ShardedConnectionProvider provider, Pattern tagPattern) {
        super(provider, tagPattern);
    }

    public Response<Object> sendCommand(ProtocolCommand cmd, String... args) {
        return sendCommand(new CommandArguments(cmd).addObjects((Object[]) args));
    }

    public Response<Object> sendCommand(ProtocolCommand cmd, byte[]... args) {
        return sendCommand(new CommandArguments(cmd).addObjects((Object[]) args));
    }

    public Response<Object> sendCommand(CommandArguments args) {
        return executeCommand(new CommandObject<>(args, BuilderFactory.RAW_OBJECT));
    }

    public <T> Response<T> executeCommand(CommandObject<T> command) {
        return appendCommand(command);
    }
}
