import io.grpc.netty.shaded.io.grpc.netty.NegotiationType;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.tapdata.data.ob.ObReadLogServerGrpc;
import io.tapdata.data.ob.ReadLogRequest;
import io.tapdata.data.ob.ReadLogResponse;
import io.tapdata.data.ob.ReaderSource;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

public class ObReadLogServiceTest {
    @Test
    public void testStart() {
        ObReadLogServerGrpc.ObReadLogServerBlockingStub blockingStub = ObReadLogServerGrpc.newBlockingStub(NettyChannelBuilder.forAddress("120.24.225.243", 50051)
                .maxInboundMessageSize(Integer.MAX_VALUE)
                .negotiationType(NegotiationType.PLAINTEXT).build());
        Iterator<ReadLogResponse> iterator = blockingStub.startReadLog(ReadLogRequest.newBuilder().setSource(
                        ReaderSource.newBuilder().setRootserverList("120.24.225.243:2882:2881,120.25.175.199:2882:2881,39.108.116.186:2882:2881").setClusterUser("root@sys").setClusterPassword("Gotapd8!").setTbWhiteList("*.*.*").build()).setTaskId("17726521f97b4478900add0e6cbee716")
                .build());
        while (iterator.hasNext()) {
            ReadLogResponse response = iterator.next();
            if(response.getPayload().getOpValue() == 6) {
                System.out.println(response);
            }
        }

    }
}
