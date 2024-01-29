package io.tapdata.connector.gauss;

import com.huawei.opengauss.jdbc.core.ConnectionFactory;
import com.huawei.opengauss.jdbc.core.QueryExecutor;
import com.huawei.opengauss.jdbc.core.v3.CopyDualImpl;
import com.huawei.opengauss.jdbc.core.v3.replication.V3PGReplicationStream;
import com.huawei.opengauss.jdbc.replication.LogSequenceNumber;
import com.huawei.opengauss.jdbc.replication.PGReplicationStream;
import com.huawei.opengauss.jdbc.replication.ReplicationType;
import com.huawei.opengauss.jdbc.replication.fluent.logical.LogicalStreamBuilder;
import com.huawei.opengauss.jdbc.util.HostSpec;
import org.postgresql.PGProperty;

import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class LogicalReplicationDemo {
    private static com.huawei.opengauss.jdbc.jdbc.PgConnection conn = null;
    private static final Properties properties = new Properties();
    //此处配置数据库IP以及端口，这里的端口为haPort，通常默认是所连接DN的port+1端口
    private static final String sourceURL = "jdbc:opengauss://121.37.171.158:8000/postgres";
    private static final String slotName = "slot1";
    private static final String user = "root";
    private static final String pwd = "Gotapd8!";
    private static final String db = "postgres";
    private static final int testMode = 2;

    public static void main(String[] args) {
        loadDrive("com.huawei.opengauss.jdbc.Driver");
        PGProperty.USER.set(properties, user);
        PGProperty.PASSWORD.set(properties, pwd);
        //对于逻辑复制，以下三个属性是必须配置项
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(properties, "9.2");
        PGProperty.REPLICATION.set(properties, db);
        PGProperty.PREFER_QUERY_MODE.set(properties, "simple");
        try {
            conn = (com.huawei.opengauss.jdbc.jdbc.PgConnection) DriverManager.getConnection(sourceURL, properties);
            System.out.println("connection success!");
            switch (testMode) {
                case 1:
                    createSlot();break;
                case 2:
                    startSlot();break;
                case 3:
                    deleteSlot();break;
                default:
                    return;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void startSlot() throws SQLException, InterruptedException {
        //开启此模式前需要创建复制槽
        com.huawei.opengauss.jdbc.replication.LogSequenceNumber waitLSN = LogSequenceNumber.valueOf("0/CA72060");
        QueryExecutor queryExecutor = ConnectionFactory.openConnection(
                new HostSpec[]{new HostSpec("121.37.171.158", 8000)},
                "root",
                "postgres",
                properties);
        CopyDualImpl copyDual = new CopyDualImpl();
        V3PGReplicationStream replicationStream = new V3PGReplicationStream(
                copyDual,
                waitLSN,
                3000,
                ReplicationType.LOGICAL
        );
        LogicalStreamBuilder slot1 = new LogicalStreamBuilder(back -> replicationStream);
        read(queryExecutor.getReplicationProtocol().startLogical(slot1));
    }

    private static void read(PGReplicationStream stream) throws SQLException, InterruptedException {
        while (true) {
            ByteBuffer byteBuffer = stream.readPending();
            if (byteBuffer == null) {
                TimeUnit.MILLISECONDS.sleep(5L);
                continue;
            }
            int offset = byteBuffer.arrayOffset();
            byte[] source = byteBuffer.array();
            int length = source.length - offset;
            System.out.println(new String(source, offset, length));
            //如果需要flush lsn，根据业务实际情况调用以下接口
            //LogSequenceNumber lastRecv = stream.getLastReceiveLSN();
            //stream.setFlushedLSN(lastRecv);
            //stream.forceUpdateStatus();
        }
    }


    public static void createSlot() throws SQLException {
        conn.getReplicationAPI()
                .createReplicationSlot()
                .logical()
                .withSlotName(slotName) //这里字符串如包含大写字母则会自动转化为小写字母
                .withOutputPlugin("mppdb_decoding")
                .make();
    }

    public static void deleteSlot() throws SQLException {
        conn.getReplicationAPI().dropReplicationSlot(slotName);
    }

    public static void loadDrive(String driver) {
        try {
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }
}
