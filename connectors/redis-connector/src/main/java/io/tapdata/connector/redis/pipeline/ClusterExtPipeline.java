package io.tapdata.connector.redis.pipeline;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.providers.ClusterConnectionProvider;

import java.util.Set;

public class ClusterExtPipeline extends ClusterPipeline {

    public ClusterExtPipeline(Set<HostAndPort> clusterNodes, JedisClientConfig clientConfig) {
        super(clusterNodes, clientConfig);
    }

    public ClusterExtPipeline(Set<HostAndPort> clusterNodes, JedisClientConfig clientConfig, GenericObjectPoolConfig<Connection> poolConfig) {
        super(clusterNodes, clientConfig, poolConfig);
    }

    public ClusterExtPipeline(ClusterConnectionProvider provider) {
        super(provider);
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
