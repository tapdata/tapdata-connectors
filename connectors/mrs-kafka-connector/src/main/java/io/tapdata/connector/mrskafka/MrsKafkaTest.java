package io.tapdata.connector.mrskafka;

import com.alibaba.fastjson.JSON;
import io.tapdata.common.CommonDbConfig;
import io.tapdata.common.CommonDbTest;
import io.tapdata.connector.mrskafka.admin.DefaultAdmin;
import io.tapdata.connector.mrskafka.config.MrsAdminConfiguration;
import io.tapdata.connector.mrskafka.config.MrsKafkaConfig;
import io.tapdata.connector.mrskafka.core.MrsKafkaService;
import io.tapdata.kit.EmptyKit;
import io.tapdata.pdk.apis.entity.TestItem;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static io.tapdata.base.ConnectorBase.testItem;

public class MrsKafkaTest extends CommonDbTest {
    private List<String> WITHOUT_READ_PRIVILEGE = new ArrayList<>();
    private List<String> WITHOUT_WRITE_PRIVILEGE = new ArrayList<>();

    private MrsKafkaConfig mrsKafkaConfig;
    private MrsKafkaService mrsKafkaService;

    public MrsKafkaTest(MrsKafkaConfig mrsKafkaConfig, Consumer<TestItem> consumer, MrsKafkaService mrsKafkaService, CommonDbConfig config) {
        super(config, consumer);
        this.mrsKafkaConfig = mrsKafkaConfig;
        this.mrsKafkaService = mrsKafkaService;
    }

    @Override
    public Boolean testHostPort() {
        TestItem testHostAndPort = mrsKafkaService.testHostAndPort();
        consumer.accept(testHostAndPort);
        if (testHostAndPort.getResult() == TestItem.RESULT_FAILED) {
            return false;
        }
        return true;
    }

    @Override
    public Boolean testVersion() {
        return true;
    }

    @Override
    public Boolean testConnect() {
        TestItem testConnect = mrsKafkaService.testConnect();
        consumer.accept(testConnect);
        if (testConnect.getResult() == TestItem.RESULT_FAILED) {
            return false;
        }
        return true;
    }

    @Override
    public Boolean testWritePrivilege() {
        MrsAdminConfiguration configuration = new MrsAdminConfiguration(mrsKafkaConfig, mrsKafkaService.getConnectorId());
        try (
                DefaultAdmin defaultAdmin = new DefaultAdmin(configuration)
        ) {
            String user = mrsKafkaConfig.getMqUsername();
            if (EmptyKit.isEmpty(user)) {
                consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
                return true;
            }
            AdminClient adminClient = defaultAdmin.getAdminClient();
            ResourcePatternFilter resourcePatternFilter = new ResourcePatternFilter(ResourceType.TOPIC, user, PatternType.ANY);
            AclBindingFilter ANY = new AclBindingFilter(resourcePatternFilter, AccessControlEntryFilter.ANY);
            DescribeAclsResult describeAclsResult = adminClient.describeAcls(ANY);
            try {
                Collection<AclBinding> aclBindings = describeAclsResult.values().get();
                if (aclBindings.isEmpty()) {
                    consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
                    return true;
                }
                for (AclBinding get : aclBindings) {
                    if ("DENY".equalsIgnoreCase(get.entry().permissionType().toString())) {
                        if ("WRITE".equalsIgnoreCase(get.entry().operation().toString())) {
                            WITHOUT_WRITE_PRIVILEGE.add(get.pattern().name());
                        } else if ("READ".equalsIgnoreCase(get.entry().operation().toString())) {
                            WITHOUT_READ_PRIVILEGE.add(get.pattern().name());
                        }
                    }
                }
            } catch (Exception e) {
                // org.apache.kafka.common.errors.ClusterAuthorizationException: Request Request(processor=2, connectionId=192.168.208.3:9092-192.168.208.1:55768-72, session=Session(User:cdc,/192.168.208.1), listenerName=ListenerName(SASL_PLAINTEXT), securityProtocol=SASL_PLAINTEXT, buffer=null) is not authorized.
                // 允许用户 "your-username" 执行 DESCRIPT_ACLS 操作，配置如：
                // acl.allow.describe.acls=user:your-username:DescribeAcls
                if (null != e.getMessage() && Pattern.matches("^org.apache.kafka.common.errors.ClusterAuthorizationException: Request Request.*is not authorized\\.$", e.getMessage())) {
                    consumer.accept(testItem("Describe ACLs", TestItem.RESULT_SUCCESSFULLY_WITH_WARN, "Please add permission 'DescribeAcls' to user '" + user + "'"));
                    return false;
                }
                throw e;
            }
            if (WITHOUT_WRITE_PRIVILEGE.size() > 0) {
                consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED, JSON.toJSONString(WITHOUT_WRITE_PRIVILEGE)));
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED, e.getMessage()));
            return false;
        }

        consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY));
        return true;
    }

    public Boolean testReadPrivilege() {
        if (WITHOUT_WRITE_PRIVILEGE.size() > 0) {
            consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_FAILED, JSON.toJSONString(WITHOUT_WRITE_PRIVILEGE)));
            return false;
        }
        consumer.accept(testItem(TestItem.ITEM_READ, TestItem.RESULT_SUCCESSFULLY));
        return true;
    }

    public Boolean testStreamRead() {
        consumer.accept(testItem(TestItem.ITEM_READ_LOG, TestItem.RESULT_SUCCESSFULLY));
        return true;
    }

}
