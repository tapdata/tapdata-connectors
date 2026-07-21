package io.tapdata.connector.hudi;

import io.tapdata.common.CommonDbTest;
import io.tapdata.connector.hudi.config.HudiConfig;
import io.tapdata.constant.DbTestItem;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.util.NetUtil;
import org.apache.commons.lang3.StringUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static io.tapdata.base.ConnectorBase.testItem;

public class HudiTest extends CommonDbTest {
    private final HudiConfig hudiConfig;

    public HudiTest(HudiConfig config, Consumer<TestItem> consumer) {
        super(config, consumer);
        this.hudiConfig = config;
        jdbcContext = new HudiJdbcContext(config);
        testFunctionMap.remove("testWritePrivilege");
    }

    @Override
    public Boolean testOneByOne() {
        return super.testOneByOne();
    }

    @Override
    public Boolean testHostPort() {
        String nameSrvAddr = hudiConfig.getNameSrvAddr();
        List<String> address = new ArrayList<>();
        if(nameSrvAddr.contains(",")) {
            address = Arrays.asList(nameSrvAddr.split(","));
        }else {
            address.add(nameSrvAddr);
        }
        StringBuilder failedHostPort = new StringBuilder();
        for (String addr : address) {
            String host = addr.split(":")[0];
            String port = addr.split(":")[1];
            try {
                NetUtil.validateHostPortWithSocket(String.valueOf(host), Integer.valueOf(port));
            } catch (Exception e) {
                failedHostPort.append(host).append(":").append(port).append(",");
            }
        }
        if (StringUtils.isNotBlank(failedHostPort)) {
            consumer.accept(testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_FAILED, failedHostPort.toString()));
            return false;
        }
        consumer.accept(testItem(DbTestItem.HOST_PORT.getContent(), TestItem.RESULT_SUCCESSFULLY));
        return true;
    }
}
