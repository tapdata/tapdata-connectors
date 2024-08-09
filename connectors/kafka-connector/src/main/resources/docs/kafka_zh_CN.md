# **1. 支持版本**

- Scala：`2.12`
- kafka：`2.0` ~ `2.5`

# **2. 支持的数据类型**

- 对象：对应 `TapMap`
- 数组：对应 `TapArray`
- 数字：对应 `TapNumber`
- 布尔：对应 `TapBoolean`
- 字符：对应 `TapString`

# **3. 功能限制/注意事项/能力边界**

- 仅支持 `JSON Object` 字符串的消息格式 (如 `{"id":1, "name": "张三"}`)
- 如果选择「忽略非JSON对象格式消息」或「忽略推送消息异常」，则仍然会记录这些消息的 `offset`，即是后续不会推送这些消息，存在数据丢失风险
- 消息推送实现为 `At least once`，对应的消费端要做好幂等操作
- 消息压缩类型：默认 `gzip`，大流量消息开启压缩可以提高传输效率
- `Kafka` 本身没有 `DML`、`DDL` 区分：
    - 场景 `Oracle` >> `Kafka` >> `MySQL`，支持 `Oracle` 的 `DDL`、`DML` 同步
    - 暂不支持由其它产品同步到 `Kafka` 的 `DDL` 同步场景

# **4. ACK 确认机制**

- 不确认（`0`）：消息推送后退出，不会检查 kafka 是否已经正确写入
- 仅写入 Master 分区（`1`）：消息推送后，会保证主分区接收并写入
- 写入大多数 ISR 分区（`-1`，默认选项）：消息推送后，会保证数据被超过一半的分区接收并写入
- 写入所有 ISR 分区（`all`）：消息推送后，会保证数据被所有分区接收并写入

# **5. 认证方式**

## **5.1 用户密码**

- 不填时，为无认证模式，使用 `org.apache.kafka.common.security.plain.PlainLoginModule` 配置
- 填写时，会使用 `org.apache.kafka.common.security.scram.ScramLoginModule` 配置

## **5.2 Kerberos**

1. 获取 Kerberos Ticket
    - 可通过 `kinit -kt <keytab-file> <principal>`、`kadmin.local -q "ktadd -norandkey -k <keytab-file> <principal>"` 获取
        - `keytab-file`：为文件名
        - `principal`：为主体配置名
        - `-norandkey` 不变更密码
    - 可通过 `klist`、`kadmin.local -q "list_principals"` 查看配置
2. 根据 Ticket 信息配置
    - 密钥表示文件，如：`kdc.keytab`
    - 配置文件，如：`kdc.conf`
    - 主体配置，如：`kafka/kkz.com@EXAMPLE.COM`
    - 服务名，如：`kafka`

### **5.2.1 常见问题**

1. 提示域未配置，如：`kkz.com`
    - 登录 `FE` 所在机器
    - 修改 `/etc/hosts` 配置：
    ```shell
    cat >> /etc/hosts << EOF
    192.168.1.2 kkz.com
    EOF
    ```
